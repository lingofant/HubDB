#include <hubDB/DBMyBufferMgr.h>
#include <hubDB/DBException.h>
#include <hubDB/DBMonitorMgr.h>

using namespace HubDB::Manager;
using namespace HubDB::Exception;

LoggerPtr DBMyBufferMgr::logger(Logger::getLogger("HubDB.Buffer.DBMyBufferMgr"));
int myBMgr = DBMyBufferMgr::registerClass();

extern "C" void * createDBMyBufferMgr(int nArgs,va_list ap);

DBMyBufferMgr::DBMyBufferMgr(bool doThreading, int cnt) :
        DBBufferMgr(doThreading, cnt),
        bcbList(NULL) {
    if (logger != NULL)
        LOG4CXX_INFO(logger, "DBMyBufferMgr()");

    bcbList = new DBBCB *[maxBlockCnt];
    for (int i = 0; i < maxBlockCnt; i++) {
        bcbList[i] = NULL;
    }

    ageBits = new unsigned int[maxBlockCnt];
    // init is already done
    gloCnt = 1;

    mapSize = cnt / 32 + 1;
    bitMap = new int[mapSize];

    for (int i = 0; i < mapSize; ++i) {
        bitMap[i] = 0;
    }

    for (int i = 0; i < cnt; ++i) {
        setBit(i);
    }

    if (logger != NULL)
        LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
}

DBMyBufferMgr::~DBMyBufferMgr() {
    LOG4CXX_INFO(logger, "~DBMyBufferMgr()");
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
    if (bcbList != NULL) {
        for (int i = 0; i < maxBlockCnt; ++i) {
            if (bcbList[i] != NULL) {
                try {
                    flushBCBBlock(*bcbList[i]);
                } catch (DBException &e) {}
                delete bcbList[i];
            }
        }
        delete[] bcbList;
        delete[] bitMap;
        delete[] ageBits;
    }
}

string DBMyBufferMgr::toString(string linePrefix) const {
    stringstream ss;
    ss << linePrefix << "[DBMyBufferMgr]" << endl;
    ss << DBBufferMgr::toString(linePrefix + "\t");
    lock();

    uint i, sum = 0;
    for (i = 0; i < maxBlockCnt; ++i) {
        if (getBit(i) == 1)
            ++sum;
    }
    ss << linePrefix << "unfixedPages( size: " << sum << " ):" << endl;
    for (i = 0; i < maxBlockCnt; ++i) {
        if (getBit(i) == 1)
            ss << linePrefix << i << endl;
    }

    ss << linePrefix << "bcbList( size: " << maxBlockCnt << " ):" << endl;
    for (int i = 0; i < maxBlockCnt; ++i) {
        ss << linePrefix << "bcbList[" << i << "]:";
        if (bcbList[i] == NULL)
            ss << "NULL" << endl;
        else
            ss << endl << bcbList[i]->toString(linePrefix + "\t");
    }
    ss << linePrefix << "-------------------" << endl;

    ss << linePrefix << "ageBits( size: " << maxBlockCnt << " ):" << endl;
    for (int i = 0; i < maxBlockCnt; ++i) {
        ss << linePrefix << "ageBits[" << i << "]:";
        ss << endl << ageBits[i]; // ->toString(linePrefix + "\t");
    }
    ss << linePrefix << "-------------------" << endl;

    ss << "Current global counter" << gloCnt;

    unlock();

    return ss.str();
}

int DBMyBufferMgr::registerClass() {
    setClassForName("DBMyBufferMgr", createDBMyBufferMgr);
    return 0;
}

DBBCB *DBMyBufferMgr::fixBlock(DBFile &file, BlockNo blockNo, DBBCBLockMode mode, bool read) {
    LOG4CXX_INFO(logger, "fixBlock()");
    LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
    LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(blockNo));
    LOG4CXX_DEBUG(logger, "mode: " + DBBCB::LockMode2String(mode));
    LOG4CXX_DEBUG(logger, "read: " + TO_STR(read));
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

    int i = findBlock(file, blockNo);

    LOG4CXX_DEBUG(logger, "i:" + TO_STR(i));

    if (i == -1) {

	    int minAgeIdx = maxBlockCnt;
	
        for (i = 0; i < maxBlockCnt; ++i) {
            // search for the oldest unfixed block
            unsigned int minAge = gloCnt;
            if (getBit(i) == 1 && ageBits[i] < minAge) {
                minAgeIdx = i;
                minAge = ageBits[i];
				if( minAge == 0 )
					break; // no entry can be older than this!
            }

        }
		
		i = minAgeIdx;
		
        if (i == maxBlockCnt)
            throw DBBufferMgrException("no more free pages");

        if (bcbList[i] != NULL) {
            if (bcbList[i]->getDirty() == false)
                flushBCBBlock(*bcbList[i]);
            delete bcbList[i];
        }
        bcbList[i] = new DBBCB(file, blockNo);
        if (read == true)
            fileMgr.readFileBlock(bcbList[i]->getFileBlock());
    }

    DBBCB *rc = bcbList[i];
    if (rc->grantAccess(mode) == false) {
        rc = NULL;
    } else {
        unsetBit(i);
    }

    LOG4CXX_DEBUG(logger, "rc: " + TO_STR(rc));
    ageBits[i] = gloCnt++; // in case of fire check this
	// check the overflow
	if ( gloCnt == max_unsigned_int_size ) {
		// just reset the counter :)
		// discuss later or just remove this
		for( int j = 0; j < maxBlockCnt; j++ ) {
			ageBits[j] = 0;
		}
		
		gloCnt = 0;
	}
    return rc;
}

void DBMyBufferMgr::unfixBlock(DBBCB &bcb) {
    LOG4CXX_INFO(logger, "unfixBlock()");
    LOG4CXX_DEBUG(logger, "bcb:\n" + bcb.toString("\t"));
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
    bcb.unlock();
    int i = findBlock(&bcb);
    if (bcb.getDirty() == true) {
        delete bcbList[i];
        bcbList[i] = NULL;
        setBit(i);
    } else if (bcb.isUnlocked() == true) {
        setBit(i);
    }
}

bool DBMyBufferMgr::isBlockOfFileOpen(DBFile &file) const {
    LOG4CXX_INFO(logger, "isBlockOfFileOpen()");
    LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
    for (int i = 0; i < maxBlockCnt; ++i) {
        if (bcbList[i] != NULL && bcbList[i]->getFileBlock() == file) {
            LOG4CXX_DEBUG(logger, "rc: true");
            return true;
        }
    }
    LOG4CXX_DEBUG(logger, "rc: false");
    return false;
}

void DBMyBufferMgr::closeAllOpenBlocks(DBFile &file) {
    LOG4CXX_INFO(logger, "closeAllOpenBlocks()");
    LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
    for (int i = 0; i < maxBlockCnt; ++i) {
        if (bcbList[i] != NULL && bcbList[i]->getFileBlock() == file) {
            if (bcbList[i]->isUnlocked() == false)
                throw DBBufferMgrException("can not close fileblock because it is still lock");
            flushBCBBlock(*bcbList[i]);
            delete bcbList[i];
            bcbList[i] = NULL;
            setBit(i);
        }
    }
}

int DBMyBufferMgr::findBlock(DBFile &file, BlockNo blockNo) {
    LOG4CXX_INFO(logger, "findBlock()");
    int pos = -1;
    for (int i = 0; pos == -1 && i < maxBlockCnt; ++i) {
        if (bcbList[i] != NULL &&
            bcbList[i]->getFileBlock() == file &&
            bcbList[i]->getFileBlock().getBlockNo() == blockNo)
            pos = i;
    }
    LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
    return pos;
}

int DBMyBufferMgr::findBlock(DBBCB *bcb) {
    LOG4CXX_INFO(logger, "findBlock()");
    int pos = -1;
    for (int i = 0; pos == -1 && i < maxBlockCnt; ++i) {
        if (bcbList[i] == bcb)
            pos = i;
    }
    LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
    return pos;
}

extern "C" void *createDBMyBufferMgr(int nArgs,va_list ap) {
    DBMyBufferMgr *b = NULL;
    bool t;
    uint c;
    switch (nArgs) {
        case 1:
            t = va_arg(ap, int);
            b = new DBMyBufferMgr(t);
            break;
        case 2:
            t = va_arg(ap, int);
            c = va_arg(ap, int);
            b = new DBMyBufferMgr(t, c);
            break;
        default:
            throw DBException("Invalid number of arguments");
    }
    return b;
}
