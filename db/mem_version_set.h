#ifndef STORAGE_LEVELDB_DB_MEM_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_MEM_VERSION_SET_H_

#include <set>
#include <vector>

#include "table/two_level_iterator.h"
#include "db/dbformat.h"
#include "db/mem_version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "db/version_set.h"

namespace leveldb {


class MemCompaction;
class Iterator;
class MemTable;
class MemVersion;
class MemVersionSet;

int FindTable(const InternalKeyComparator& icmp,
             const std::vector<MemTableMetaData*>& tables, const Slice& key);

//memversion
class MemVersion {
 public:

  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get( const LookupKey& k,
                    std::string* value);
  void Ref();
  void Unref();

  //get compact input[level] tables for several times
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<MemTableMetaData*>* inputs);

  int NumTables(int level) const { return tables_[level].size(); }

  bool NeedCompactWithLock(){
    if(BaseLevel()==config::kNumMemLevels-1){
      //最底层
      if(compaction_level_==config::kNumMemLevels-2){
        return true;
      }else return false;
    }
    else{
      //不是最底层如m1，则m0-m1和m1-m2都会受影响？
      if(compaction_level_==BaseLevel()||compaction_level_==BaseLevel()-1){
        return true;
      }else return false;
    }
  }

  int BaseLevel(){//返回有表的最下层
    int level=0;
    for(int i = 0; i <= config::kNumMemLevels-1; i++){
      if(tables_[i].size() > 0) level = std::max(level ,i);
    }
    return level;
  }

  std::string DebugString() const;

 private:
  friend class MemCompaction;
  friend class MemVersionSet;

  class LevelTableNumIterator;

  explicit MemVersion(MemVersionSet* mvset)
      : mvset_(mvset),
        next_(this),
        prev_(this),
        compaction_score_(-1),
        compaction_level_(-1),
        flush_score_(-1),
        refs_(0) {}

  MemVersion(const MemVersion&) = delete;
  MemVersion& operator=(const MemVersion&) = delete;

  ~MemVersion();


  void ForEachOverlapping(Slice user_key,Slice internal_key, void* arg,
                          bool (*func)(void*,MemTableMetaData*));

  MemVersionSet* mvset_;
  MemVersion* next_;
  MemVersion* prev_;
  int refs_;

  std::vector<MemTableMetaData*> tables_[config::kNumMemLevels];//dont include m0 memtable

  //size compaction,only l1 to l2
  double compaction_score_;
  int compaction_level_;//压缩的层

  double flush_score_;
};

//memversionset
class MemVersionSet {
 public:
  MemVersionSet(const std::string& dbname, const Options* options,
             const InternalKeyComparator*,VersionSet* versions);
  MemVersionSet(const MemVersionSet&) = delete;
  MemVersionSet& operator=(const MemVersionSet&) = delete;

  ~MemVersionSet();

  void Apply(MemVersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  void Finalize(MemVersion* v);
  void AddLiveTables(std::set<uint64_t>* live);
  
  MemVersion* current() const { return current_; }

  //table number
  uint64_t NewTableNumber() { return next_table_number_++; }

  //call by makeroomfor
  void ReuseTableNumber(uint64_t table_number) {
    if (next_table_number_ == table_number + 1) {
      next_table_number_ = table_number;
    }
  }
    // Return the last sequence number.
  uint64_t LastSequence() const { return versions_->LastSequence(); }

  // number of Table at the specified level.
  int NumLevelTables(int level) const;

  int64_t NumLevelBytes(int level) const;

  //get compaction input
  MemCompaction* PickCompaction();
  MemTableMetaData* PickFlush();//选择mem刷到磁盘
  //iterator
  Iterator* MakeInputIterator(MemCompaction* c);

  bool NeedsCompaction() const {
    MemVersion* v = current_;
    return (v->compaction_score_ >= 1);
  }
  bool NeedsFlush() const {// flush
    return current_->flush_score_>= 1;
  }

 private:
  class Builder;
  
  friend class MemCompaction;
  friend class MemVersion;
  VersionSet* versions_;
  //compact l1,so dont need
  //void Finalize(MemVersion* v);

  void GetRange(const std::vector<MemTableMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<MemTableMetaData*>& inputs1,
                 const std::vector<MemTableMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);
  void SetupOtherInputs(MemCompaction* c);

  void AppendVersion(MemVersion* v);

  const std::string dbname_;
  const Options* const options_;
  const InternalKeyComparator icmp_;
  uint64_t next_table_number_;

  MemVersion dummy_versions_;
  MemVersion* current_;

  std::string compact_pointer_[config::kNumMemLevels];
  
  std::string flush_pointer_;

};

//l1 to l2 memcompaction
class MemCompaction {
 public:
  ~MemCompaction();

  MemVersionEdit* edit() { return &edit_; }
  int level() const { return level_; }

  int num_input_tables(int which) const { return inputs_[which].size(); }

  MemTableMetaData* input(int which, int i) const { return inputs_[which][i]; }

  uint64_t MaxOutputTableSize() const { return max_output_table_size_; }

  void AddInputDeletions(MemVersionEdit* m_edit);

  void ReleaseInputs();

  bool IsTrivialMove() const;
 private:
  friend class MemVersion;
  friend class MemVersionSet;

  MemCompaction(const Options* options,int level);

  int level_;

  uint64_t max_output_table_size_;
  MemVersion* input_version_;
  MemVersionEdit edit_;

  std::vector<MemTableMetaData*> inputs_[2];  // The two sets of inputs
  bool seen_key_;             // Some output key has been seen

};
}  // namespace leveldb
#endif
