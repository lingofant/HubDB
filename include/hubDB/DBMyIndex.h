#ifndef DBMYINDEX_H_
#define DBMYINDEX_H_

#include <hubDB/DBIndex.h>

namespace HubDB{
	namespace Index{
		class DBMyIndex : public DBIndex{
			
			public:
				DBMyIndex(DBBufferMgr & bufferMgr,DBFile & file,enum AttrTypeEnum attrType,ModType mode,bool unique);
				~DBMyIndex();
				string toString(string linePrefix="") const;
				
				void initializeIndex();
				void find(const DBAttrType & val,DBListTID & tids);
				void insert(const DBAttrType & val,const TID & tid);
				void remove(const DBAttrType & val,const DBListTID & tid);
				bool isIndexNonUniqueAble(){ return false;};
				void unfixBACBs(bool dirty);				
				
				static int registerClass();
				
			private:
				static const uint MAX_TID_PER_ENTRY;
				const uint tidsPerEntry; 
				bool findFirstPage(const DBAttrType & val,BlockNo & blockNo);
				uint entriesPerPage()const;
				void genFirstLast(const char * ptr);
				DBAttrType & first() { return *first_;};
				DBAttrType & last() { return *last_;};
				
				bool isEmpty(const char * ptr);
                void insertEmptyPage(BlockNo pos);
                void removeEmptyPage(BlockNo pos);
                void insertInPage(const DBAttrType & val,const TID & tids,BlockNo pos);
                void findFromPage(const DBAttrType & val,BlockNo pos,list<TID> & tids);
                void removeFromPage(const DBAttrType & val,list<TID> tid,BlockNo pos);
			
	  			static LoggerPtr logger;
				static const BlockNo rootBlockNo;
				stack<DBBACB> bacbStack;
				DBAttrType * first_;
				DBAttrType * last_;
		};
	}
}


#endif /*DBMYINDEX_H_*/
