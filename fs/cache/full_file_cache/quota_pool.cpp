/*
Copyright 2022 The Photon Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "quota_pool.h"

#include <dirent.h>
#include <sys/stat.h>

#include <list>

#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/fs/path.h>

#include "quota_store.h"



namespace photon {
namespace fs {

const int64_t kGB = 1024ll * 1024 * 1024;
const int64_t kMaxDirFreeSpace = 15ll * kGB;
const int64_t kEvictionMark = 2ll * kGB;
const int64_t kDefaultQuota = 16ll * 1024 * kGB; //正常默认值

void QuotaFilePool::DirInfo::calcMark(QuotaFilePool* pool) {
  waterMark = pool->calcWaterMark(quota, kMaxDirFreeSpace);
  riskMark = std::max(quota - kEvictionMark, (waterMark + quota) >> 1);
}

QuotaFilePool::QuotaFilePool(IFileSystem* mediaFs, uint64_t capacityInGB,
  uint64_t periodInUs, uint64_t diskAvailInBytes, uint64_t refillUnit, int quotaDirLevel)
  : FileCachePool(mediaFs, capacityInGB, periodInUs, diskAvailInBytes, refillUnit),
    quotaDirLevel_(quotaDirLevel) {
}

ICacheStore* QuotaFilePool::do_open(std::string_view pathname, int flags, mode_t mode) {
  auto localFile = openMedia(pathname, flags, mode);
  if (!localFile) {
    return nullptr;
  }

  auto pos = getQuotaCtrlPos(pathname.data());
  if (!pos) {
    LOG_ERROR_RETURN(EINVAL, nullptr, "pathname don't contain dir name:`", pathname)
  }
  std::string dirName(pathname.data() + 1, pos);
  auto dir = dirInfos_.find(dirName);

  // Check cold container first
  auto coldIt = coldIndex_.find(pathname);
  if (coldIt != coldIndex_.end()) {
    promoteToHot(coldIt);
  }

  auto find = fileIndex_.find(pathname);
  if (find == fileIndex_.end()) {
    find = insertNewFile(dir, std::move(dirName), pathname).first;
    auto lruEntry = static_cast<QuotaLruEntry*>(find->second.get());
    lruEntry->openCount = 1;
  } else {
    lru_.access(find->second->lruIter);
    auto lruEntry = static_cast<QuotaLruEntry*>(find->second.get());
    dir->second.lru.access(lruEntry->QuotaLruIter);
    find->second->openCount++;
  }

  // If hot LRU exceeds the limit, demote the LRU tail (only if not open) to cold
  while (lru_.size() > hotLruLimit_) {
    auto tailIter = lru_.back();
    if (tailIter->second->openCount == 0) {
      demoteToCold(tailIter);
    } else {
      break;
    }
  }

  return new QuotaFileStore(this, localFile, refillUnit_, find);
}

const char* QuotaFilePool::getQuotaCtrlPos(const char* pathname) {
  char* begin = const_cast<char*>(pathname);
  for (int i = 0; i != quotaDirLevel_; ++i) {
    begin = strchr(begin + 1, '/');
    if (!begin) {
      return nullptr;
    }
  }
  return begin;
}

std::pair<QuotaFilePool::FileIterator, QuotaFilePool::DirIter> QuotaFilePool::insertNewFile(
    DirIter dir, std::string&& dirName, std::string_view file) {
  if (dir == dirInfos_.end()) {
    DirInfo info;
    info.quota = kDefaultQuota;
    info.calcMark(this);
    dir = dirInfos_.emplace(std::move(dirName), std::move(info)).first;
  }
  auto QuotaLruIter = dir->second.lru.push_front(fileIndex_.end());
  auto lruIter = lru_.push_front(fileIndex_.end());

  std::unique_ptr<QuotaLruEntry> entry(new QuotaLruEntry{lruIter, 0, QuotaLruIter, 0, dir});
  auto find = fileIndex_.emplace(file, std::move(entry)).first;
  lru_.front() = find;
  dir->second.lru.front() = find;

  dir->second.fileCount++;
  return {find, dir};
}

void QuotaFilePool::updateDirLru(FileIterator iter) {
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;
  dirInfo.lru.access(lruEntry->QuotaLruIter);
}

bool QuotaFilePool::dirSpaceIsFull(FileIterator iter) {
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;
  return dirInfo.inEvicting;
}

//  currently, we exist duplicate pwrite
void QuotaFilePool::updateDirSpace(FileIterator iter, uint64_t diff) {
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;
  dirInfo.used += diff;
  if (dirInfo.used > dirInfo.riskMark) {
    dirInfo.inEvicting = true;
  }
}

void QuotaFilePool::updateDirQuota(FileIterator iter, size_t quota) {
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;
  dirInfo.quota = quota;
  dirInfo.calcMark(this);
}

int QuotaFilePool::set_quota(std::string_view pathname, size_t quota) {
  auto pos = getQuotaCtrlPos(pathname.data());
  std::string dirName(pathname.data() + 1, pos);
  auto find = dirInfos_.find(dirName.c_str());
  if (dirInfos_.end() == find) {
    DirInfo info;
    info.quota = quota;
    info.calcMark(this);
    find = dirInfos_.emplace(std::move(dirName), std::move(info)).first;
    return 0;
  }
  find->second.quota = quota;
  find->second.calcMark(this);
  return 0;
}

int QuotaFilePool::stat(CacheStat* stat, std::string_view pathname) {
  stat->refill_unit = refillUnit_;
  if (pathname.empty() || pathname == "/") {
    stat->total_size = capacityInGB_ * kGB / refillUnit_;
    stat->used_size = totalUsed_;
    stat->used_size /= refillUnit_;
  } else if ('/' == pathname.back()) {
    auto pos = getQuotaCtrlPos(pathname.data());
    std::string dirName(pathname.data() + 1, pos);
    auto find = dirInfos_.find(dirName.c_str());
    if (find != dirInfos_.end()) {
      stat->used_size = find->second.used / refillUnit_;
      stat->total_size = find->second.quota / refillUnit_;
    }
  } else {
    struct stat st = {};
    auto ret = mediaFs_->stat(pathname.data(), &st);
    if (ret) {
      LOG_ERRNO_RETURN(0, ret, "stat failed, ret:`,name:`", ret, pathname);
    }
    stat->used_size = st.st_blocks * kDiskBlockSize / refillUnit_;
    stat->total_size = st.st_size / refillUnit_;
  }
  return 0;
}

int QuotaFilePool::evict(std::string_view filename) {
  // Check cold container first — cold files are not in fileIndex_
  auto coldIt = coldIndex_.find(filename);
  if (coldIt != coldIndex_.end()) {
    return evictColdByIndex(coldIt->second) >= 0 ? 0 : -1;
  }

  auto pos = getQuotaCtrlPos(filename.data());
  std::string dirName(filename.data() + 1, pos);
  auto find = dirInfos_.find(dirName.c_str());
  if (dirInfos_.end() == find) {
    return 0;
  }
  auto& lru = find->second.lru;
  auto fileIter = fileIndex_.find(filename);
  if (fileIter == fileIndex_.end()){
    LOG_WARN("No such file , name: `", filename.data());
    return 0;
  }
  const auto& filePath = fileIter->first;
  int err;
  auto lruEntry = static_cast<QuotaLruEntry*>(fileIter->second.get());

  {
    auto cacheStore = static_cast<FileCacheStore*>(open(filePath, O_RDWR, 0644));
    DEFER(cacheStore->release());
    photon::scoped_rwlock rl(cacheStore->rw_lock(), photon::WLOCK);
    lru.mark_key_cleared(lruEntry->QuotaLruIter);
    err = mediaFs_->truncate(filePath.data(), 0);
    if (err) {
      ERRNO e;
      LOG_ERROR("truncate(0) failed, name : `, ret : `, error code : `", filePath, err, ERRNO());
      if (e.no == EINTR) {
        return 0;
      }
    }
    afterFtrucate(fileIter);
  }
  photon::thread_yield();
  return 0;
}

void QuotaFilePool::dirEviction() {
  std::list<DirInfo*> evictInfos;
  // first stop write
  for (auto& dir : dirInfos_) {
    auto& dirInfo = dir.second;
    if (dirInfo.used > dirInfo.waterMark) {
      dirInfo.inEvicting = true;
      evictInfos.push_back(&dirInfo);
    } else {
      dirInfo.inEvicting = false;
    }
  }

  // second start evicting, completely fair eviction
  auto cur = evictInfos.begin();
  while (!evictInfos.empty() && !exit_) {
    auto dir = *cur;
    if (!dir->lru.empty() && dir->used > dir->waterMark) {
      auto fileIter = dir->lru.back();
      const auto& fileName = fileIter->first;
      auto lruEntry = static_cast<QuotaLruEntry*>(fileIter->second.get());
      int err;
      bool flags_dir_delete = false;

      {
        auto cacheStore = static_cast<FileCacheStore*>(open(fileName, O_RDWR, 0644));
        DEFER(cacheStore->release());
        photon::scoped_rwlock rl(cacheStore->rw_lock(), photon::WLOCK);
        if (lruEntry->openCount==0){
          dir->lru.mark_key_cleared(lruEntry->QuotaLruIter);
        } else {
          dir->lru.access(lruEntry->QuotaLruIter);
        }
        err = mediaFs_->truncate(fileName.data(), 0);
      }

      if (err) {
        ERRNO e;
        LOG_ERROR("truncate(0) failed, name : `, ret : `, error code : `", fileName, err, e);
        if (e.no == EINTR) {
          continue;
        }
      }
      flags_dir_delete = afterFtrucate(fileIter);
      photon::thread_yield();
      if (flags_dir_delete){
        cur = evictInfos.erase(cur);
      } else {
        cur++;
      }
    } else {
      dir->inEvicting = false;
      cur = evictInfos.erase(cur);
    }
    if (cur == evictInfos.end()) {
      cur = evictInfos.begin();
    }
  }
}

bool QuotaFilePool::afterFtrucate(FileIterator iter) {
  bool ret = false;
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;
  totalUsed_ -= static_cast<int64_t>(lruEntry->size);
  dirInfo.used -= static_cast<int64_t>(lruEntry->size);
  lruEntry->size = 0;
  if (dirInfo.used < 0) {
    dirInfo.used = 0;
  }
  if (totalUsed_ < 0) {
    totalUsed_ = 0;
  }
  if (0 == iter->second->openCount) {
    auto err = mediaFs_->unlink(iter->first.data());
    if (err) {
      ERRNO e;
      LOG_ERROR("unlink failed, name : `, ret : `, error code : `", iter->first, err, e);
      if (err && (e.no == EBUSY)) {
        return false;
      }
    }
    lru_.remove(iter->second->lruIter);
    dirInfo.lru.remove(lruEntry->QuotaLruIter);
    dirInfo.fileCount--;
    if (0 == dirInfo.fileCount) {
        dirInfos_.erase(lruEntry->dir); // TODO(suoshi.yf): when to clear dir info?
        ret = true;
    }
    fileIndex_.erase(iter);
  }
  return ret;
}

void QuotaFilePool::eviction() {
  dirEviction();
  FileCachePool::eviction();
}

int QuotaFilePool::insertFile(std::string_view file) {
  struct stat st = {};
  auto ret = mediaFs_->stat(file.data(), &st);
  if (ret) {
    LOG_ERRNO_RETURN(0, -1, "stat failed, name : `", file.data());
  }
  auto fileSize = st.st_blocks * kDiskBlockSize;

  auto pos = getQuotaCtrlPos(file.data());
  if (!pos) {
    LOG_ERRNO_RETURN(0, -1, "path don't contain dir name : `", file.data());
  }
  std::string dirName(file.data() + 1, pos);
  auto dir = dirInfos_.find(dirName);
  auto pair = insertNewFile(dir, std::move(dirName), file);
  auto find = pair.first;
  dir = pair.second;
  auto lruEntry = static_cast<QuotaLruEntry*>(find->second.get());
  lruEntry->size = fileSize;
  dir->second.used += fileSize;
  totalUsed_ += fileSize;
  return 0;
}

// Demote a hot-LRU entry to cold container.
// Keep file in dir LRU for accurate dir eviction and stats.
// Store dir LRU iterator in quotaCold_ to avoid linear search during promote/evict.
void QuotaFilePool::demoteToCold(FileNameMap::iterator iter) {
  auto lruEntry = static_cast<QuotaLruEntry*>(iter->second.get());
  auto& dirInfo = lruEntry->dir->second;

  // Get the iterator to the file in dir LRU before we lose access to lruEntry
  auto dirLruIter = lruEntry->QuotaLruIter;

  // Add to quota cold container with demoteTime and dir LRU iterator
  uint32_t idx = static_cast<uint32_t>(quotaCold_.size());
  auto nodeIt = coldIndex_.emplace(iter->first, idx);
  quotaCold_.emplace_back(nodeIt.first, lruEntry->size, photon::now, dirLruIter);

  // Only remove from global hot LRU, keep in dir LRU
  lru_.remove(lruEntry->lruIter);
  // Note: dir->lru and fileCount are kept for dir eviction to work correctly

  // Remove from fileIndex_
  fileIndex_.erase(iter);
}

// Promote a cold entry back to hot LRU.
// Use saved dir LRU iterator from quotaCold_ to avoid linear search.
void QuotaFilePool::promoteToHot(ColdIndexMap::iterator iter) {
  uint32_t idx = iter->second;
  uint64_t size = quotaCold_[idx].size;
  std::string_view filename = iter->first;
  auto dirLruIter = quotaCold_[idx].dirLruIter;

  auto pos = getQuotaCtrlPos(filename.data());
  if (!pos) {
    LOG_ERROR_RETURN(EINVAL, void(), "filename don't contain dir name:`", filename);
  }
  std::string dirName(filename.data() + 1, pos);
  auto dirIt = dirInfos_.find(dirName);
  if (dirIt == dirInfos_.end()) {
    LOG_ERROR_RETURN(EINVAL, void(), "dir not found during promote, filename: `", filename);
  }

  // Reuse existing quotaLruIter, only add to global hot LRU
  auto lruIter = lru_.push_front(fileIndex_.end());
  std::unique_ptr<QuotaLruEntry> entry(new QuotaLruEntry{lruIter, 0, dirLruIter, size, dirIt});
  auto newIter = fileIndex_.emplace(filename, std::move(entry)).first;
  lru_.front() = newIter;
  // Note: dir->lru and fileCount are kept unchanged

  // Remove from cold container using swap-remove
  coldIndex_.erase(iter);
  if (idx + 1 < quotaCold_.size()) {
    std::swap(quotaCold_[idx], quotaCold_.back());
    quotaCold_[idx].iter->second = idx;
  }
  quotaCold_.pop_back();
}

// Evict a cold entry by index.
// Use saved dir LRU iterator from quotaCold_ to avoid linear search.
// Returns the evicted file size, or -1 on error.
ssize_t QuotaFilePool::evictColdByIndex(uint32_t idx) {
  assert(idx < quotaCold_.size());
  auto &entry = quotaCold_[idx];
  std::string_view filename = entry.iter->first;
  uint64_t fileSize = entry.size;
  auto dirLruIter = entry.dirLruIter;

  auto pos = getQuotaCtrlPos(filename.data());
  if (!pos) {
    LOG_ERROR_RETURN(EINVAL, -1, "filename don't contain dir name:`", filename);
  }
  std::string dirName(filename.data() + 1, pos);
  auto dirIt = dirInfos_.find(dirName);
  if (dirIt == dirInfos_.end()) {
    LOG_ERROR_RETURN(EINVAL, -1, "dir not found during promote, filename: `", filename);
  }

  if (dirIt != dirInfos_.end()) {
    auto& dirInfo = dirIt->second;
    // Remove from dir LRU using saved iterator - O(1)!
    dirInfo.lru.remove(dirLruIter);
    dirInfo.fileCount--;
    dirInfo.used -= static_cast<int64_t>(fileSize);
    if (dirInfo.used < 0) dirInfo.used = 0;
  }

  if (fileSize > 0) {
    int err = mediaFs_->truncate(filename.data(), 0);
    if (err) {
      ERRNO e;
      LOG_ERRNO_RETURN(0, -1, "truncate(0) failed (cold), name : `", filename);
    }
    totalUsed_ -= static_cast<int64_t>(fileSize);
    entry.size = 0;
    if (totalUsed_ < 0) totalUsed_ = 0;
  }
  int err = mediaFs_->unlink(filename.data());
  if (err) {
    ERRNO e;
    LOG_ERRNO_RETURN(0, -1, "unlink failed (cold), name : `", filename);
  }

  // Remove from cold container using swap-remove
  coldIndex_.erase(entry.iter);
  if (idx + 1 < quotaCold_.size()) {
    std::swap(quotaCold_[idx], quotaCold_.back());
    quotaCold_[idx].iter->second = idx;
  }
  quotaCold_.pop_back();
  return static_cast<ssize_t>(fileSize);
}

// Evict cold entries until needEvict bytes are freed.
// Uses sampling to find the oldest cold entries.
uint64_t QuotaFilePool::evictColdWhenFull(uint64_t needEvict) {
  uint64_t evictSize = 0;
  while (evictSize < needEvict && !quotaCold_.empty() && !exit_) {
    uint32_t sz = quotaCold_.size();
    // Fixed candidates: head (0) and tail (sz-1), plus up to 5 random indices.
    static const int kMaxCandidates = 7;
    uint32_t old = 0;
    uint64_t oldTime = quotaCold_[old].demoteTime;
    if (quotaCold_.back().demoteTime < oldTime) {
      old = sz - 1;
      oldTime = quotaCold_[old].demoteTime;
    }
    for (int k = 2; k < kMaxCandidates; k++) {
      uint32_t candidate = rand() % sz;
      if (quotaCold_[candidate].demoteTime < oldTime) {
        old = candidate;
        oldTime = quotaCold_[old].demoteTime;
      }
    }
    auto r = evictColdByIndex(old);
    if (r >= 0) evictSize += r;
    photon::thread_yield();
  }
  return evictSize;
}

}
}