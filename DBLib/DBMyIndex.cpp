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

int leaf_size = 4;
int sizeOfHead = sizeof(bool) * 2 + sizeof(int) + sizeof(TID);
string tree;

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
    int foo = rootBlockNo;
    bacbStack.push(bufMgr.fixBlock(file, foo, LOCK_EXCLUSIVE));
    char *swap_ptr = bacbStack.top().getDataPtr();
    TID rootTID;
    rootTID.read(swap_ptr);
    if (rootTID.page != 0) {
        bacbStack.pop();
        bacbStack.push(bufMgr.fixBlock(file, rootTID.page, LOCK_EXCLUSIVE));

    }

    //ree = printTree();

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

void DBMyIndex::emtpyBACBs() {
    while (bacbStack.empty() == false) {
        try {
            if (bacbStack.top().getModified()) {
                bacbStack.top().setModified();
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
    search_in_node(val, tids);

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
    search_in_node(val, tid, rootTID, nullptr, false);
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


/** Split node of tree
 * A Node is a Middle-Node... so no leaf or root
 * @param val
 * @param tid
 * @param node
 * @param parent_ptr
 * @param islast
 * @return
 */
DBMyIndex::value_container DBMyIndex::split_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast) {



    return DBMyIndex::value_container();
}



/**
 * Split Root Node if the root node is not a leaf
 */
DBMyIndex::value_container DBMyIndex::split_root(const DBAttrType &val, const TID &tid, TID node) {

    //Read Values of Page
    char *ptr = bacbStack.top().getDataPtr();
    char *leafptr = ptr;

    int block_nr = bacbStack.top().getBlockNo();
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
    char *old_level_ptr = ptr;
    ptr += sizeof(int);

    TID next;
    next.read(ptr);
    char *next_ptr = ptr;
    ptr += sizeof(TID);
    char *old_values_ptr = ptr;


    TID new_node_next;
    //Initialize new node (for the smaller values of the acutal root)
    TID new_node_tid = initNode(false, isleaf, new_node_next);
    char *newnode_ptr = bacbStack.top().getDataPtr();
    char *new_values_ptr = newnode_ptr + sizeof(bool) * 2 + sizeof(int) + sizeof(TID);
    char *new_next_ptr = newnode_ptr + sizeof(bool) * 2 + sizeof(int);
    DBAttrType *searchval;
    bool inserted = false;
    //Copy small Values in New Node
    for (int i = 0; i < fill_level / 2; i++) {
        searchval = DBAttrType::read(ptr, attrType);
        if (val.operator>(*searchval) || inserted) {
            searchval->write(new_values_ptr);
            ptr += sizeof(val);
            new_values_ptr += sizeof(val);
            TID tid_swap;
            tid_swap.read(ptr);
            tid_swap.write(new_values_ptr);
            ptr += sizeof(TID);
            new_values_ptr += sizeof(TID);
        } else {
            val.write(new_values_ptr);
            ptr += sizeof(val);
            new_values_ptr += sizeof(val);
            tid.write(new_values_ptr);
            ptr += sizeof(TID);
            new_values_ptr += sizeof(TID);
            inserted = true;
        }
    }

    //Take the value of the middle and safe it for the new root
    DBAttrType *root_val;
    searchval = DBAttrType::read(ptr, attrType);
    if (val.operator>(*searchval) || inserted) {
        root_val = DBAttrType::read(ptr, attrType);
        ptr += sizeof(val);
        TID swap_tid;
        swap_tid.read(ptr);
        swap_tid.write(new_next_ptr);
        ptr += sizeof(TID);
    } else {
        *root_val = val;
        tid.write(new_next_ptr);
    }

    //Copy the bigger values to the beginning of the old node
    for (int i = 0; i < fill_level / 2; i++) {
        searchval = DBAttrType::read(ptr, attrType);
        if (val.operator>(*searchval) || inserted) {
            searchval->write(old_values_ptr);
            ptr += sizeof(val);
            old_values_ptr += sizeof(val);
            TID tid_swap;
            tid_swap.read(ptr);
            tid_swap.write(old_values_ptr);
            ptr += sizeof(TID);
            old_values_ptr += sizeof(TID);
        } else {
            val.write(old_values_ptr);
            old_values_ptr += sizeof(val);
            tid.write(old_values_ptr);
            old_values_ptr += sizeof(TID);
            inserted = true;
        }
    }

    isroot = false;
    fill_level = fill_level / 2;

    //Set new values of old root
    memcpy(isrootptr, &isroot, sizeof(bool));
    memcpy(old_level_ptr, &fill_level, sizeof(int));

    //Set Values of new node
    fill_level = leaf_size - fill_level;
    char * new_node_fill_level = newnode_ptr + sizeof(bool) * 2;
    // memcpy (*destination, *source, size);
    memcpy(new_node_fill_level, &fill_level, sizeof(int));

    //Save Pages and Empty Buffer
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();

    //Initialize new Root
    TID rootnext;
    rootnext.page = block_nr;
    TID new_root_tid = initNode(true, false, rootnext);
    char *newroot_ptr = bacbStack.top().getDataPtr();
    char *root_values_ptr = newroot_ptr + sizeof(bool) * 2 + sizeof(int) + sizeof(TID);

    //Save Middle-Value in Root
    root_val->write(root_values_ptr);
    root_values_ptr += sizeof(val);
    new_node_tid.write(root_values_ptr);

    int root_fill = 1;
    char * new_root_fill_level = newroot_ptr + sizeof(bool) * 2;
    // memcpy (*destination, *source, size);
    memcpy(new_root_fill_level, &root_fill, sizeof(int));


    //Reload new Root
    bacbStack.top().setModified();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
    bacbStack.push(bufMgr.fixBlock(file, new_root_tid.page, LOCK_EXCLUSIVE));


    tree = printTree();
    //Return empty value container
    return DBMyIndex::value_container();

}


DBMyIndex::value_container
DBMyIndex::split_leaf(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast) {
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
    char *old_level_ptr = ptr;
    ptr += sizeof(int);

    TID next;
    next.read(ptr);
    char *next_ptr = ptr;
    ptr += sizeof(TID);


    char *start_values_ptr = ptr;
    char *swap_ptr = ptr;

    TID new_node_tid = initNode(false, isleaf, next);
    char *newnode_ptr = bacbStack.top().getDataPtr();
    char *new_values_ptr = newnode_ptr + sizeof(bool) * 2 + sizeof(int) + sizeof(TID);

    //Copy small Values in New Node
    for (int i = 0; i < fill_level / 2; i++) {
        DBAttrType::read(swap_ptr, attrType)->write(new_values_ptr);
        swap_ptr += sizeof(val);
        new_values_ptr += sizeof(val);
        TID tid;
        tid.read(swap_ptr);
        tid.write(new_values_ptr);
        swap_ptr += sizeof(TID);
        new_values_ptr += sizeof(TID);
    }

    DBAttrType *big_nod_val;
    big_nod_val = DBAttrType::read(swap_ptr, attrType);
    for (int i = 0; i < (fill_level - fill_level / 2); i++) {
        DBAttrType::read(swap_ptr, attrType)->write(start_values_ptr);
        swap_ptr += sizeof(val);
        start_values_ptr += sizeof(val);
        TID tid;
        tid.read(swap_ptr);
        tid.write(start_values_ptr);
        swap_ptr += sizeof(TID);
        start_values_ptr += sizeof(TID);
    }

    //Fill Levels
    newnode_ptr += sizeof(bool) * 2;
    int level = fill_level / 2;
    memcpy(newnode_ptr, &level, sizeof(int));
    level = fill_level - fill_level / 2;
    memcpy(old_level_ptr, &level, sizeof(int));

    //Next Pointers
    newnode_ptr += sizeof(int);
    node.write(newnode_ptr);



//Define Retun value, bigger leaf first value with smaller leaf TID (cause the tree uses smaller than for childs)
    vc.val = big_nod_val;
    vc.isnew = true;
    vc.tid = new_node_tid;

    //write new value in leaf
    if (vc.val->operator<(val)) {
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
        insert_into_node(val, tid, node, parent_ptr, false);
        bacbStack.push(bufMgr.fixBlock(file, node.page, LOCK_EXCLUSIVE));
    } else {
        insert_into_node(val, tid, new_node_tid, parent_ptr, islast);
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
    }


    return vc;
}


/**
 * Split the root for the first time
 * @param val
 * @param tid
 * @param node
 * @param parent_ptr
 * @param islast
 * @return
 */
DBMyIndex::value_container DBMyIndex::split_root_leaf(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast) {
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

    //ToDo: Evt in Split root überführen

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
    char *next_ptr = ptr;
    ptr += sizeof(TID);


    //Pointer auf die zu kopierenden werte
    if (!isleaf) {
        ptr += (sizeof(val) + sizeof(tid)) * leaf_size / 2 - 1 + sizeof(val);
        memcpy(next_ptr, ptr, sizeof(TID));
        ptr += sizeof(TID);
    } else {
        ptr += (sizeof(val) + sizeof(tid)) * leaf_size / 2;
    }


    TID node_tid = initNode(false, isleaf, next);
    char *newnode_ptr = bacbStack.top().getDataPtr();
    if (isleaf) {
        node_tid.write(next_ptr);

    }
    newnode_ptr += sizeof(bool) * 2;
    memcpy(newnode_ptr, &fill_level, sizeof(int));
    newnode_ptr += sizeof(int) + sizeof(TID);

    memmove(newnode_ptr, ptr, (sizeof(val) + sizeof(tid)) * (leaf_size - leaf_size / 2));
    if (!islast) {
        vc.val = DBAttrType::read(newnode_ptr, attrType);
        vc.tid = node_tid;
    } else {
        parent_ptr += sizeof(bool) * +sizeof(int) + sizeof(TID);
        node_tid.write(parent_ptr);
    }


    vc.isnew = true;

    if (vc.val->operator<(val)) {
        insert_into_node(val, tid, node_tid, parent_ptr, islast);

    } else {
        bacbStack.top().setModified();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
        insert_into_node(val, tid, node, parent_ptr, false);

    }


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

DBMyIndex::value_container
DBMyIndex::insert_into_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast) {
    struct value_container vc;
    vc.tid.page - 1;

    bool leftie = false;

    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());


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
    char *fill_level_ptr = ptr;
    memcpy(&fill_level, ptr, sizeof(int));

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
            if (*cnt == 0 || i == fill_level) {
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
            TID tid;
            tid.read(end_position);
            tid.write(swap_pointer + sizeof(val));
            end_position -= sizeof(val);
            DBAttrType::read(end_position, attrType)->write(swap_pointer);

        } while (end_position != insert_position);

    }
    if (reached_end) {
        val.write(end_position);
        end_position += sizeof(val);
        tid.write(end_position);
        fill_level += 1;
        memcpy(fill_level_ptr, &fill_level, sizeof(int));
        bacbStack.top().setModified();
    } else {
        if (isleaf && !isroot) {
            vc = split_leaf(val, tid, node, parent_ptr, islast);
        }

        else if(isroot && !isleaf){
            vc = split_root(val, tid, node);
        }
        else {
            vc = split_root_leaf(val, tid, node, parent_ptr, islast);
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


            }

        }
    }
    // memcpy (*destination, *source, size);

    int page_swap = bacbStack.top().getBlockNo();
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
    if (isroot) {
        bacbStack.push(bufMgr.fixBlock(file, page_swap, LOCK_EXCLUSIVE));
    }
    return vc;



    /*     * einfügen von wert in leaf
     * if overflow
     *      new_leaf_address = splitnode
     *      entscheidung in welches leaf eingefügt werden soll
     *              insert_in_node
     * return new_leaf_address;
     *
     */

}

DBMyIndex::value_container
DBMyIndex::search_in_node(const DBAttrType &val, const TID &tid, TID node, char *parent_ptr, bool islast) {
    /* in node nach wert suchen
     *      if leaf
     *          new_leaf_address  = einfügen_in_node
     *      if node
     *          search_in_node
     * if new_leaf_address
     *      insert_in_node
     */



    if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
        bufMgr.upgradeToExclusive(bacbStack.top());


    char *ptr = bacbStack.top().getDataPtr();

    parent_ptr = ptr;

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
                vc = insert_into_node(val, tid, node, parent_ptr, false);
                reached_end = true;
                found = true;
            } else {
                bacbStack.push(bufMgr.fixBlock(file, nextNode.page, LOCK_EXCLUSIVE));
                vc = search_in_node(val, tid, nextNode, parent_ptr, true);
                found = true;
            }
            break;
        } else {
            searchval = DBAttrType::read(ptr, attrType);
            if (val.operator<(*searchval) && !found) {
                found = true;
                insert_position = ptr;
                if (isleaf) {
                    vc = insert_into_node(val, tid, node, parent_ptr, islast);
                    break;
                } else {
                    TID found_node;
                    found_node.read(ptr + sizeof(val));
                    bacbStack.push(bufMgr.fixBlock(file, found_node.page, LOCK_EXCLUSIVE));
                    vc = search_in_node(val, tid, found_node, parent_ptr, false);
                    break;
                }
            }
        }
        ptr += (sizeof(val) + sizeof(tid));
        cnt = (uint *) ptr;
    }

    if (isleaf && !found && !reached_end) {
        vc = insert_into_node(val, tid, node, parent_ptr, false);
    } else if (!isleaf && !found) {
        vc = search_in_node(val, tid, nextNode, parent_ptr, true);
    }

    if (vc.isnew) {
        if (bacbStack.empty()) bacbStack.push(bufMgr.fixBlock(file, node.page, LOCK_EXCLUSIVE));
        insert_into_node(*vc.val, vc.tid, node, parent_ptr, islast);
        vc.isnew = false;
    }


    if (!bacbStack.empty()) {
        bufMgr.unfixBlock(bacbStack.top());
    }


    tree = printTree();
    return DBMyIndex::value_container();
}

string DBMyIndex::printTree() {
    stringstream ss;
    ss << "[DBMyIndex]" << endl;
    for (int i = 0; i < bufMgr.getBlockCnt(file); i++) {
        bacbStack.push(bufMgr.fixBlock(file, i, LOCK_SHARED));
        ss << printNode();
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
    }
    return ss.str();

}

string DBMyIndex::printNode() {
    stringstream ss;
    ss << "Node:" << endl;


    char *ptr = bacbStack.top().getDataPtr();


    if (bacbStack.top().getBlockNo() == 0) {
        ptr += sizeof(TID);
    }


    int page = bacbStack.top().getBlockNo();
    ss << "Page: " << page << endl;
    char *leafptr = ptr;
    bool isroot;
    // memcpy (*destination, *source, size);
    memcpy(&isroot, ptr, sizeof(bool));
    ptr += sizeof(bool);
    ss << "Root:" << isroot << endl;

    bool isleaf;
    memcpy(&isleaf, ptr, sizeof(bool));
    ptr += sizeof(bool);
    ss << "Leaf:" << isleaf << endl;

    int fill_level;
    memcpy(&fill_level, ptr, sizeof(int));
    ptr += sizeof(int);
    ss << "Fill Level:" << fill_level << endl;


    TID nextNode;
    nextNode.read(ptr);
    ptr += sizeof(TID);
    ss << "Next Node:" << nextNode.page << endl;


    // memcpy (*destination, *source, size);

    uint *cnt = (uint *) ptr;

    DBAttrType *searchval;

    DBListTID tids;

    for (int i = 0; i < fill_level; ++i) {

        DBAttrType *val;
        val = DBAttrType::read(ptr, attrType);
        ptr += sizeof(val);

        TID tid;
        tid.read(ptr);
        ptr += sizeof(TID);
        tids.push_back(tid);
        ss << i << endl;
        ss << " " << val->toString("/n") << "   TID:    " << tid.page << endl;
    }
    ss << endl;
    return ss.str();


}

/**
 * Used for Finding a Value in Tree and returning the tid of the touple
 * @param val
 * @param tids
 */
void DBMyIndex::search_in_node(const DBAttrType &val, DBListTID &tids) {

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

    bool found = false;
    for (int i = 0; i < fill_level; ++i) {
        searchval = DBAttrType::read(ptr, attrType);
        if (searchval->operator>(val) && !isleaf) {
            TID found_tid;
            found_tid.read(ptr + sizeof(val));

            bacbStack.push(bufMgr.fixBlock(file, found_tid.page, LOCK_SHARED));

            search_in_node(val, tids);
            found = true;
            break;
        } else if (searchval->operator==(val) && isleaf) {
            TID found_tid;
            found_tid.read(ptr + sizeof(val));
            tids.push_back(found_tid);
            found = true;
            break;
        }
        ptr += (sizeof(val) + sizeof(TID));
    }
    if (!found && !isleaf) {
        bacbStack.push(bufMgr.fixBlock(file, nextNode.page, LOCK_SHARED));
        search_in_node(val, tids);
    }

    if (!isroot) {
        bufMgr.unfixBlock(bacbStack.top());
        bacbStack.pop();
    }

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


