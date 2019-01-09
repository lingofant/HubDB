/**
 * already finished:
 * createDBIndex (wird von DBTypes bei der initialisierung aufgerufen) => initialisiert die STandardwerte
 * DBMyIndex-Konstruktor (Schaut ob Indexfile bereits existiert und lädt dieses)
 * Inditialize Index (wird vom Konstruktor aufgerufen wenn Index-File noch nicht existiert) => schreibt file auf platte
 *
 * Nice To Know:
 * bacbStack ist ein STack der die gefixten Blöcke im Buffer speichert
 * bacbStack.top() muss immer die Rood Node sein
 */


#include <hubDB/DBMyIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBMyIndex::logger(Logger::getLogger("HubDB.Index.DBMyIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rMyIdx = DBMyIndex::registerClass();
// set static const rootBlockNo to 0 in DBMyIndex Class - (BlockNo=uint)
const BlockNo DBMyIndex::rootBlockNo(0);
// max this amount of entries with the some value in the index - WHEN NOT UNIQUE
const uint DBMyIndex::MAX_TID_PER_ENTRY(20);
// Funktion bekannt machen
extern "C" void *createDBMyIndex(int nArgs, va_list ap);

int leaf_size = 10;
int sizeOfHead = sizeof(bool) * 2 + sizeof(int) + sizeof(TID);


/**
 * Ausgabe des Indexes zum Debuggen
 */
string DBMyIndex::toString(string linePrefix) const {
    stringstream ss;
    ss << linePrefix << "[DBMyIndex]" << endl;
    ss << DBIndex::toString(linePrefix + "\t") << endl;
    ss << linePrefix << "tidsPerEntry: " << tidsPerEntry << endl;
    ss << linePrefix << "entriesPerPage: " << entriesPerPage() << endl;
    ss << linePrefix << "-----------" << endl;
    return ss.str();
}

/** Konstruktor
 * - DBBufferMgr & bufferMgr (Referenz auf Buffermanager)
 * - DBFile & file (Referenz auf Dateiobjekt)
 * - enum AttrTypeEnum (Typ des Indexattributs)
 * - ModType mode (Accesstyp: READ, WRITE - siehe DBTypes.h)
 * - bool unique (ist Attribute unique)
 */
DBMyIndex::DBMyIndex(DBBufferMgr &bufferMgr, DBFile &file,
                     enum AttrTypeEnum attrType, ModType mode, bool unique) :
// call base constructor
        DBIndex(bufferMgr, file, attrType, mode, unique),
        // wenn unqiue dann nur ein Tupel pro value, ansonsten den wert vom oben gesetzen MAX_TID_PER_ENTRY
        tidsPerEntry(unique == true ? 1 : MAX_TID_PER_ENTRY),
        // DBAttrType first_ und last_ = NULL
        // diese beiden Werte dienen um den ersten und letzten Wert eines Blockes
        // temporaer zwischenzuspeichern --> siehe genFirstLast()
        //ToDo: Brauchen wir den?
        first_(NULL), last_(NULL) {
    if (logger != NULL) {
        LOG4CXX_INFO(logger, "DBMyIndex()");
    }

    assert(entriesPerPage() > 1);

    //	if(unique == false && isIndexNonUniqueAble() == false)
    //		throw HubDB::Exception::DBIndexException("set up nonunique but index does not support it");

    // if this function is called for the first time -> index file has 0 blocks
    // -> call initializeIndex to create file
    if (bufMgr.getBlockCnt(file) == 0) {
        LOG4CXX_DEBUG(logger, "initializeIndex");
        initializeIndex();
    }

    // fuege ersten Block der Indexdatei in den Stack der geblockten Bloecke hinzu
    bacbStack.push(bufMgr.fixBlock(file, rootBlockNo,
                                   mode == READ ? LOCK_SHARED : LOCK_INTWRITE));
    char *swap_ptr = bacbStack.top().getDataPtr();
    TID rootTID;
    rootTID.read(swap_ptr);
    if (rootTID.page != 0) {
        bacbStack.pop();
        bacbStack.push(bufMgr.fixBlock(file, rootTID.page,
                                       mode == READ ? LOCK_SHARED : LOCK_INTWRITE));

    }

    if (logger != NULL) {
        LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
    }
}

/**
 * Destrkctor
 * Soll first_, last_ und alle geblockten Bloecke wieder freigeben
 */
DBMyIndex::~DBMyIndex() {
    LOG4CXX_INFO(logger, "~DBMyIndex()");
    unfixBACBs(false);
    if (first_ != NULL)
        delete first_;
    if (last_ != NULL)
        delete last_;
}

/**
 * Freigeben aller vom Index geblockten Bloecke
 */
void DBMyIndex::unfixBACBs(bool setDirty) {
    LOG4CXX_INFO(logger, "unfixBACBs()");
    LOG4CXX_DEBUG(logger, "setDirty: " + TO_STR(setDirty));
    LOG4CXX_DEBUG(logger, "bacbStack.size()= " + TO_STR(bacbStack.size()));
    while (bacbStack.empty() == false) {
        try {
            if (bacbStack.top().getModified()) {
                if (setDirty == true)
                    bacbStack.top().setDirty();
            }
            bufMgr.unfixBlock(bacbStack.top());
        } catch (DBException e) {
        }
        bacbStack.pop();
    }
}

/**
 * Gibt die Anzahl der Eintraege pro Seite zurueck
 *
 * Gesamtblockgroesse: DBFileBlock::getBlockSize()
 * Platz zum Speichern der Position des ersten freien Blockes: sizeof(uint)
 * Laenge des Schluesselattributs: DBAttrType::getSize4Type(attrType)
 * Groesse der TID (siehe DBTypes.h): sizeof(TID)
 * Eintraege (TIDs) pro Schluesseleintrag (maximal MAX_TID_PER_ENTRY or 1 when unique): tidsPerEntry
 *
 * Bsp.: VARCHAR: entriesPerPage() = (1024-4) / (30+8*20) = 5, Rest 70
 * Bsp.: INTEGER UNIQUE: entriesPerPage() = (1024-4) / (4+8*1) = 85, Rest 0
 */
uint DBMyIndex::entriesPerPage() const {
    //ToDo: Ändern der entries per Page berechnung auf blätter

    return (DBFileBlock::getBlockSize() - sizeof(uint)) /
           (DBAttrType::getSize4Type(attrType) + sizeof(TID) * tidsPerEntry);
}

/**
 * Erstelle Indexdatei.
 */
void DBMyIndex::initializeIndex() {
    LOG4CXX_INFO(logger, "initializeIndex()");
    if (bufMgr.getBlockCnt(file) != 0)
        throw DBIndexException("can not initializie exisiting table");

    try {
        // einen ersten Block der Datei erhalten und gleich fixen
        bacbStack.push(bufMgr.fixNewBlock(file));
        // modified date setzen

        TID roottid;
        roottid.page = 0;
        bool isroot = true;
        bool isleaf = true;

        bool *boolptr = &isroot;
        char *ptr = bacbStack.top().getDataPtr();
        uint *cnt = (uint *) ptr;
        if (*cnt == 0) {
            memcpy(ptr, &roottid, sizeof(TID));
            ptr += sizeof(TID);
            // memcpy (*destination, *source, size);
            memcpy(ptr, boolptr, sizeof(bool));
            ptr += sizeof(bool);
            memcpy(ptr, boolptr, sizeof(bool));
            ptr += sizeof(bool);
            int fill_level = 0;
            memcpy(ptr, &fill_level, sizeof(int));
            ptr += sizeof(int);
            TID next;
            next.page = -1;
            memcpy(ptr, &next, sizeof(TID));
        }
        bacbStack.top().setModified();
    } catch (DBException e) {
        if (bacbStack.empty() == false)
            bufMgr.unfixBlock(bacbStack.top());
        throw e;
    }
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    // nun muss die liste der geblockten Seiten wieder leer sein, sonst Abbruch
    assert(bacbStack.empty() == true);
}

/**
 * Sucht im Index nach einem bestimmten Wert
 * - const DBAttrType & val: zu suchender Schluesselwert
 * - DBListTID & tids: Referenz auf Liste von TID Objekten
 * Rueckgabewert: KEINER, aber die Funktion aendert die uebergebene Liste
 * von TID Objekten (siehe DBTypes.h: typedef list<TID> DBListTID;)
 */
void DBMyIndex::find(const DBAttrType &val, DBListTID &tids) {
    LOG4CXX_INFO(logger, "find()");
    LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

    // ein Block muss geblockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");

    // Löschen der uebergebenen Liste ("Returnliste")
    tids.clear();

    //ToDo: Implement Search in Tree and fill List tids
}

/**
 * Einfuegen eines Schluesselwertes (moeglicherweise bereits vorhangen)
 * zusammen mit einer Referenz auf eine TID.
 */
void DBMyIndex::insert(const DBAttrType &val, const TID &tid) {
    LOG4CXX_INFO(logger, "insert()");
    LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));
    LOG4CXX_DEBUG(logger, "tid: " + tid.toString());

    // vor Beginn der Operation darf nur genau eine Seite
    // (wie immer die nullte) gelockt sein

    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");

    // wenn diese eine Seite nicht exclusive gelockt ist, dann dies nachholen!
    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());


    TID rootTID;
    rootTID.read(bacbStack.top().getDataPtr());
    search_in_node(val, tid, rootTID);
    // am Ende der operation muss genau eine Seite gelockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");
}

/**
 * Entfernt alle Tupel aus der Liste der tids.
 * Um schneller auf der richtigen Seite mit dem Entfernen anfangen zu koennen,
 * wird zum Suchen auch noch der zu loeschende value uebergeben
 */
void DBMyIndex::remove(const DBAttrType &val, const list<TID> &tid) {
    LOG4CXX_INFO(logger, "remove()");
    LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

    // vor Beginn der Operation darf nur genau eine Seite (wie immer die nullte) gelockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");

    // wenn diese eine Seite nicht exclusive gelockt ist, dann dies nachholen!
    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());

    // wenn das Indexattribut unique ist, dann darf in der Liste TID nie mehr als ein Wert stehen
    if (unique == true && tid.size() > 1)
        throw DBIndexUniqueKeyException("try to remove multiple key but is unique index");

    // Suchen auf welcher seite Tupel mit val im IndexAttribute liegen (wenn vorhanden)
    // oder liegen koennten (wenn nicht vorhanden) und die in variable 'page' abspeichern

    //ToDo: Implementierung von suche des leafs und löschen dieser - dabei merge berücksichtigen

    // am Ende der Operation muss genau eine Seite gelockt sein
    if (bacbStack.size() != 1)
        throw DBIndexException("BACB Stack is invalid");
}


/**
 * Fuegt createDBMyIndex zur globalen factory method-map hinzu
 */
int DBMyIndex::registerClass() {
    setClassForName("DBMyIndex", createDBMyIndex);
    return 0;
}


TID DBMyIndex::initNode(bool isroot, bool isleaf, TID next) {
    //Neuen Block für Leaf
    bacbStack.push(bufMgr.fixNewBlock(file));
    char *ptr = bacbStack.top().getDataPtr();
    char *leafptr = ptr;
    uint *cnt = (uint *) ptr;
    if (*cnt == 0) {
        // memcpy (*destination, *source, size);
        memcpy(ptr, &isroot, sizeof(bool));
        ptr += sizeof(bool);
        memcpy(ptr, &isleaf, sizeof(bool));
        ptr += sizeof(bool);
        int fill_level = 0;
        memcpy(ptr, &fill_level, sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &next, sizeof(TID));
    }

    TID tid;
    tid.page = bacbStack.top().getBlockNo();
    tid.slot = 0;
    return tid;
}

DBMyIndex::value_container DBMyIndex::split_node(const DBAttrType &val, const TID &tid, TID node) {
    /*
     * new_node
     * größere hälfte in node kopieren
     * if root
     *      new_root
     *      alte_root is root false
     *      new_root insert smalles value of new_node
     *      in mgmt_varibalen root_tid = new_root_tid
     *
     * return node_tid, smallest_val_of_new_node
     */

    value_container vc;
    vc.tid.page = -1;

    char *ptr = bacbStack.top().getDataPtr();
    char *leafptr = ptr;

    if (bacbStack.top().getBlockNo() == 0) {
        ptr += sizeof(TID);
    }

    bool isroot;
    char *isrootptr = ptr;
    // memcpy (*destination, *source, size);
    memcpy(&isroot, ptr, sizeof(bool));
    ptr += sizeof(bool);

    bool isleaf;
    memcpy(&isleaf, ptr, sizeof(bool));
    ptr += sizeof(bool);

    int fill_level;
    memcpy(&fill_level, ptr, sizeof(int));
    fill_level = leaf_size / 2;
    memcpy(ptr, &fill_level, sizeof(int));
    ptr += sizeof(int);

    TID next;
    next.read(ptr);
    char * next_ptr = ptr;
    ptr += sizeof(TID);



    //Pointer auf die zu kopierenden werte
    if(!isleaf){

        ptr += (sizeof(val) + sizeof(tid)) * leaf_size / 2 -1 + sizeof(val);
        memcpy(next_ptr, ptr, sizeof(TID));
        ptr += sizeof(TID);
    }
    else{
        ptr += (sizeof(val) + sizeof(tid)) * leaf_size / 2;
    }

    TID node_tid = initNode(false, isleaf, next);
    char *newnode_ptr = bacbStack.top().getDataPtr();
    if(isleaf){
        node_tid.write(next_ptr);

    }
    newnode_ptr += sizeof(bool) * 2;
    memcpy(newnode_ptr, &fill_level, sizeof(int));
    newnode_ptr += sizeof(int) + sizeof(TID);

    vc.val = DBAttrType::read(ptr, attrType);
    vc.tid = node;
    memmove(newnode_ptr, ptr, (sizeof(val) + sizeof(tid)) * (leaf_size - leaf_size / 2));
    bacbStack.top().setModified();

    vc.isnew = true;

    if (isroot) {

        TID newroot = initNode(true, false, node_tid);
        bool f = false;
        memcpy(isrootptr, &f, sizeof(bool));
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        unfixBACBs(false);
        bacbStack.push(bufMgr.fixBlock(file, rootBlockNo, LOCK_EXCLUSIVE));
        char *swap_ptr = bacbStack.top().getDataPtr();
        newroot.write(swap_ptr);
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
        bacbStack.push(bufMgr.fixBlock(file, newroot.page, LOCK_EXCLUSIVE));
        vc.isnew = false;

    }

    return vc;
}

DBMyIndex::value_container DBMyIndex::insert_into_node(const DBAttrType &val, const TID &tid, TID node) {
    struct value_container vc;
    vc.tid.page - 1;

    bool leftie = false;

    char *ptr = bacbStack.top().getDataPtr();

    if (bacbStack.top().getBlockNo() == 0) {
        ptr += sizeof(TID);
    }

    int page = bacbStack.top().getBlockNo();

    char *leafptr = ptr;
    bool isroot;
    // memcpy (*destination, *source, size);
    memcpy(&isroot, ptr, sizeof(bool));
    ptr += sizeof(bool);

    bool isleaf;
    memcpy(&isleaf, ptr, sizeof(bool));
    ptr += sizeof(bool);

    int fill_level;
    memcpy(&fill_level, ptr, sizeof(int));
    if (fill_level < leaf_size) {
        fill_level += 1;
    }
    memcpy(ptr, &fill_level, sizeof(int));
    ptr += sizeof(int);

    TID nextNode;
    memcpy(&nextNode, ptr, sizeof(TID));
    ptr += sizeof(TID);

    // memcpy (*destination, *source, size);




    uint *cnt = (uint *) ptr;

    DBAttrType *searchval;
    char *insert_position, *end_position;


    bool found = false;
    bool reached_end = false;

    if (fill_level < leaf_size) {
        for (int i = 0; i < leaf_size; ++i) {
            if (*cnt == 0 || i == fill_level - 1) {
                reached_end = true;
                end_position = ptr;
                break;
            } else {
                searchval = DBAttrType::read(ptr, attrType);
                if (val.operator<(*searchval) && !found) {
                    found = true;
                    insert_position = ptr;
                }
            }
            ptr += (sizeof(val) + sizeof(tid));
            cnt = (uint *) ptr;

        }
    }
    if (found) {
        do {
            char *swap_pointer = end_position;
            end_position -= sizeof(tid);
            memcpy(swap_pointer + sizeof(val), end_position, sizeof(tid));
            end_position -= sizeof(val);
            memcpy(swap_pointer, end_position, sizeof(val));
        } while (end_position != insert_position);

    }
    if (reached_end) {
        val.write(end_position);
        end_position += sizeof(val);
        tid.write(end_position);
    } else {
        vc = split_node(val, tid, node);
        if (isroot) {
            char *ptr = bacbStack.top().getDataPtr();
            int root_level = 1;
            ptr += sizeof(bool) * 2;
            memcpy(ptr, &root_level, sizeof(int));
            ptr += sizeof(int) + sizeof(TID);

            vc.val->write(ptr);
            ptr += sizeof(val);
            vc.tid.page = page;
            vc.tid.write(ptr);
            bacbStack.top().setModified();
            vc.isnew = false;
            //   insert_into_node(val, tid, vc.tid);
        } else {
            if (vc.val->operator>(val)) {
                bufMgr.unfixBlock(bacbStack.top());
                bacbStack.pop();
                insert_into_node(val, tid, node);
                leftie = true;
            } else {
                insert_into_node(val, tid, vc.tid);
                bufMgr.unfixBlock(bacbStack.top());

            }
        }
    }
    // memcpy (*destination, *source, size);

    bufMgr.unfixBlock(bacbStack.top());
    if (!isroot && !leftie) {
        bacbStack.pop();
    }
    return vc;



    /*
     * einfügen von wert in leaf
     * if overflow
     *      new_leaf_address = splitnode
     *      entscheidung in welches leaf eingefügt werden soll
     *              insert_in_node
     * return new_leaf_address;
     *
     */

}

DBMyIndex::value_container DBMyIndex::search_in_node(const DBAttrType &val, const TID &tid, TID node) {
    /* in node nach wert suchen
     *      if leaf
     *          new_leaf_address  = einfügen_in_node
     *      if node
     *          search_in_node
     * if new_leaf_address
     *      insert_in_node
     */



    char *ptr = bacbStack.top().getDataPtr();


    if (bacbStack.top().getBlockNo() == 0) {
        ptr += sizeof(TID);
    }


    char *leafptr = ptr;
    bool isroot;
    // memcpy (*destination, *source, size);
    memcpy(&isroot, ptr, sizeof(bool));
    ptr += sizeof(bool);

    bool isleaf;
    memcpy(&isleaf, ptr, sizeof(bool));
    ptr += sizeof(bool);

    int fill_level;
    memcpy(&fill_level, ptr, sizeof(int));
    ptr += sizeof(int);

    TID nextNode;
    nextNode.read(ptr);
    ptr += sizeof(TID);

    // memcpy (*destination, *source, size);

    uint *cnt = (uint *) ptr;

    DBAttrType *searchval;
    char *insert_position, *end_position;


    bool found = false;
    bool reached_end = false;
    value_container vc;
    vc.tid.page = -1;

    for (int i = 0; i < leaf_size; ++i) {
                    if (*cnt == 0 || i == fill_level) {
                if (isleaf) {
                    vc = insert_into_node(val, tid, node);
                    reached_end = true;
                } else {
                    bacbStack.push(bufMgr.fixBlock(file, nextNode.page, LOCK_EXCLUSIVE));
                    vc = search_in_node(val, tid, node);
                }
                break;
            } else {
                searchval = DBAttrType::read(ptr, attrType);
                if (val.operator<(*searchval) && !found) {
                    found = true;
                    insert_position = ptr;
                    if (isleaf) {
                        vc = insert_into_node(val, tid, node);
                        break;
                    } else {
                        TID node;
                        node.read(ptr + sizeof(val));
                        bacbStack.push(bufMgr.fixBlock(file, node.page, LOCK_EXCLUSIVE));
                        vc = search_in_node(val, tid, node);
                        break;
                    }
                }
            }
            ptr += (sizeof(val) + sizeof(tid));
            cnt = (uint *) ptr;
        }

    if (isleaf && !found && !reached_end) {
        vc = insert_into_node(val, tid, node);
    } else if (!isleaf && !found) {
        vc = search_in_node(val, tid, nextNode);
    }

     if (vc.isnew){

          insert_into_node(*vc.val, vc.tid, node);
      }





    return DBMyIndex::value_container();
}


/**
 * Gerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 * - DBBufferMgr *: Buffermanager
 * - DBFile *: Dateiobjekt
 * - attrType: Attributtp
 * - ModeType: READ, WRITE
 * - bool: unique Indexattribut
 */
extern "C" void *createDBMyIndex(int nArgs, va_list ap) {
    // Genau 5 Parameter
    if (nArgs != 5) {
        throw DBException("Invalid number of arguments");
    }
    DBBufferMgr *bufMgr = va_arg(ap, DBBufferMgr *);
    DBFile *file = va_arg(ap, DBFile *);
    enum AttrTypeEnum attrType = (enum AttrTypeEnum) va_arg(ap, int);
    ModType m = (ModType) va_arg(ap, int);
    bool unique = (bool) va_arg(ap, int);
    return new DBMyIndex(*bufMgr, *file, attrType, m, unique);
}
