#ifndef DBMYINDEX_H_
#define DBMYINDEX_H_

#include <hubDB/DBIndex.h>

namespace HubDB {
    namespace Index {
        class DBMyIndex : public DBIndex {

        public:
            DBMyIndex(DBBufferMgr &bufferMgr, DBFile &file, enum AttrTypeEnum attrType, ModType mode, bool unique);

            ~DBMyIndex();

            string toString(string linePrefix = "") const;

            void initializeIndex();

            void find(const DBAttrType &val, DBListTID &tids);

            void insert(const DBAttrType &val, const TID &tid);

            void remove(const DBAttrType &val, const DBListTID &tid);

            bool isIndexNonUniqueAble() { return false; };

            void unfixBACBs(bool dirty);

            static int registerClass();

        private:
            static const uint MAX_TID_PER_ENTRY;
            const uint tidsPerEntry;
            struct value_container{
                DBAttrType *val;
                TID tid;
                bool isnew = false;
            };


            void emtpyBACBs();
            string printTree();
            string printNode();
            TID initNode(bool isroot, bool isleaf, TID next);

            void search_in_node(const DBAttrType &val, DBListTID &tids);
            value_container search_in_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast);
            value_container insert_into_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast);
            value_container split_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast);
            value_container split_leaf(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast);
            value_container split_root(const DBAttrType &val, const TID &tid, TID node);
            value_container split_root_leaf(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast);


            uint entriesPerPage() const;

            DBAttrType &first() { return *first_; };

            DBAttrType &last() { return *last_; };

            static LoggerPtr logger;

            static const BlockNo rootBlockNo;
            stack<DBBACB> bacbStack;
            DBAttrType *first_;
            DBAttrType *last_;
        };
    }
}


#endif /*DBMYINDEX_H_*/
