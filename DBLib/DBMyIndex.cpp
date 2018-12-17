#include <hubDB/DBSeqIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBSeqIndex::logger(Logger::getLogger("HubDB.Index.DBSeqIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rSeqIdx = DBSeqIndex::registerClass();
// set static const rootBlockNo to 0 in DBSeqIndex Class - (BlockNo=uint)
const BlockNo DBSeqIndex::rootBlockNo(0);
// max this amount of entries with the some value in the index - WHEN NOT UNIQUE
const uint DBSeqIndex::MAX_TID_PER_ENTRY(20);
// Funktion bekannt machen
extern "C" void * createDBSeqIndex(int nArgs, va_list & ap);

/**
 * Ausgabe des Indexes zum Debuggen
 */
string DBSeqIndex::toString(string linePrefix) const {
	stringstream ss;
	ss << linePrefix << "[DBSeqIndex]" << endl;
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
DBSeqIndex::DBSeqIndex(DBBufferMgr & bufferMgr, DBFile & file,
		enum AttrTypeEnum attrType, ModType mode, bool unique) :
			// call base constructor
			DBIndex(bufferMgr, file, attrType, mode, unique),
			// wenn unqiue dann nur ein Tupel pro value, ansonsten den wert vom oben gesetzen MAX_TID_PER_ENTRY
			tidsPerEntry(unique == true ? 1 : MAX_TID_PER_ENTRY),
			// DBAttrType first_ und last_ = NULL
			// diese beiden Werte dienen um den ersten und letzten Wert eines Blockes
			// temporaer zwischenzuspeichern --> siehe genFirstLast()
			first_(NULL), last_(NULL) {
	if (logger != NULL) {
		LOG4CXX_INFO(logger,"DBSeqIndex()");
	}

	assert(entriesPerPage()>1);

	//	if(unique == false && isIndexNonUniqueAble() == false)
	//		throw HubDB::Exception::DBIndexException("set up nonunique but index does not support it");

	// if this function is called for the first time -> index file has 0 blocks
	// -> call initializeIndex to create file
	if (bufMgr.getBlockCnt(file) == 0) {
		LOG4CXX_DEBUG(logger,"initializeIndex");
		initializeIndex();
	}

	// fuege ersten Block der Indexdatei in den Stack der geblockten Bloecke hinzu
	bacbStack.push(bufMgr.fixBlock(file, rootBlockNo,
			mode == READ ? LOCK_SHARED : LOCK_INTWRITE));

	if (logger != NULL) {
		LOG4CXX_DEBUG(logger,"this:\n"+toString("\t"));
	}
}

/**
 * Destrkctor
 * Soll first_, last_ und alle geblockten Bloecke wieder freigeben
 */
DBSeqIndex::~DBSeqIndex() {
	LOG4CXX_INFO(logger,"~DBSeqIndex()");
	unfixBACBs(false);
	if (first_ != NULL)
		delete first_;
	if (last_ != NULL)
		delete last_;
}

/**
 * Freigeben aller vom Index geblockten Bloecke
 */
void DBSeqIndex::unfixBACBs(bool setDirty) {
	LOG4CXX_INFO(logger,"unfixBACBs()");
	LOG4CXX_DEBUG(logger,"setDirty: "+TO_STR(setDirty));
	LOG4CXX_DEBUG(logger,"bacbStack.size()= "+TO_STR(bacbStack.size()));
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
uint DBSeqIndex::entriesPerPage() const {
	return (DBFileBlock::getBlockSize() - sizeof(uint)) /
			(DBAttrType::getSize4Type(attrType) + sizeof(TID) * tidsPerEntry);
}

/**
 * Erstelle Indexdatei.
 */
void DBSeqIndex::initializeIndex() {
	LOG4CXX_INFO(logger,"initializeIndex()");
	if (bufMgr.getBlockCnt(file) != 0)
		throw DBIndexException("can not initializie exisiting table");

	try {
		// einen ersten Block der Datei erhalten und gleich fixen
		bacbStack.push(bufMgr.fixNewBlock(file));
		// modified date setzen
		bacbStack.top().setModified();
	} catch (DBException e) {
		if (bacbStack.empty() == false)
			bufMgr.unfixBlock(bacbStack.top());
		throw e;
	}
	bufMgr.unfixBlock(bacbStack.top());
	bacbStack.pop();

	// nun muss die liste der geblockten Seiten wieder leer sein, sonst Abbruch
	assert(bacbStack.empty()==true);
}

/**
 * Sucht im Index nach einem bestimmten Wert
 * - const DBAttrType & val: zu suchender Schluesselwert
 * - DBListTID & tids: Referenz auf Liste von TID Objekten
 * Rueckgabewert: KEINER, aber die Funktion aendert die uebergebene Liste
 * von TID Objekten (siehe DBTypes.h: typedef list<TID> DBListTID;)
 */
void DBSeqIndex::find(const DBAttrType & val, DBListTID & tids) {
	LOG4CXX_INFO(logger,"find()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));

	// ein Block muss geblockt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// Löschen der uebergebenen Liste ("Returnliste")
	tids.clear();

	// die erste Seite suchen auf der ein Tupel mit einem bestimmten Wert vorkommen KANN
	// im Erfolgsfall wird true zurueckgegeben und page ist gefuellt
	BlockNo page;
	if (true == findFirstPage(val, page)) {
		// wenn page gefunden dann auf dieser Page weiter suchen und TID List fuellen
		findFromPage(val, page, tids);
	}

	// auch jetzt muss noch ein Block geblockt sein!
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");
}

/**
 * Einfuegen eines Schluesselwertes (moeglicherweise bereits vorhangen)
 * zusammen mit einer Referenz auf eine TID.
 */
void DBSeqIndex::insert(const DBAttrType & val, const TID & tid) {
	LOG4CXX_INFO(logger,"insert()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));
	LOG4CXX_DEBUG(logger,"tid: "+tid.toString());

	// vor Beginn der Operation darf nur genau eine Seite
	// (wie immer die nullte) gelockt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// wenn diese eine Seite nicht exclusive gelockt ist, dann dies nachholen!
	if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
		bufMgr.upgradeToExclusive(bacbStack.top());

	// Suchen auf welcher Seite Tupel mit val im IndexAttribute liegen (wenn vorhanden)
	// oder liegen koennten (wenn nicht vorhanden) und die in page abspeichern
	BlockNo page;
	findFirstPage(val, page);

	// wenn die Nummer der Seite auf der eingefuegt werden soll gefunden ist, dann EINFUEGEN
	insertInPage(val, tid, page);

	// am Ende der operation muss genau eine Seite gelockt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");
}

/**
 * Entfernt alle Tupel aus der Liste der tids.
 * Um schneller auf der richtigen Seite mit dem Entfernen anfangen zu koennen,
 * wird zum Suchen auch noch der zu loeschende value uebergeben
 */
void DBSeqIndex::remove(const DBAttrType & val, const list<TID> & tid) {
	LOG4CXX_INFO(logger,"remove()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));

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
	BlockNo page;
	if (findFirstPage(val, page) == true)
		// wenn Seite gefunden: loeschen starten
		removeFromPage(val, tid, page);
	else
		// ansonsten Fehler werfen
		throw DBIndexException("key not found (val:\n" + val.toString("\t") + ")");

	// am Ende der Operation muss genau eine Seite gelockt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");
}

/**
 * Sucht erste Seite auf der ein bestimmter Value in einem Tupel stehen KOENNTE
 */
bool DBSeqIndex::findFirstPage(const DBAttrType & val, BlockNo & blockNo) {
	LOG4CXX_INFO(logger,"findFirstPage()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));

	// wieder genau eine Seite gelockt
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// 2 bools um moegliche Sucherfolge zu speichern
	bool found = false;
	bool foundMulti = false;
	BlockNo blockMulti;

	// uebergebene Variable blockNo wird erstmal auf rootBlockNo gesetzt,
	// um bei der ersten Seite zu starten
	blockNo = rootBlockNo;

	// wenn die Seite Tupel enthalt
	if (false == isEmpty(bacbStack.top().getDataPtr())) {
		// speichere Nummer des ersten und letzten Blockes der Datei
		BlockNo firstB = rootBlockNo;
		BlockNo lastB = bufMgr.getBlockCnt(file);
		LOG4CXX_DEBUG(logger,"firstB:"+TO_STR(firstB));
		LOG4CXX_DEBUG(logger,"lastB:"+TO_STR(lastB));

		// durchlaufe binaere Suche durch die Bloecke der Datei
		// solange bis gefunden oder keine Blocke mehr zum suchen
		while (firstB < lastB && found == false) {
			blockNo = firstB + (lastB - firstB) / 2;
			LOG4CXX_DEBUG(logger,"firstB:"+TO_STR(firstB));
			LOG4CXX_DEBUG(logger,"lastB:"+TO_STR(lastB));
			LOG4CXX_DEBUG(logger,"blockNo:"+TO_STR(blockNo));

			// wenn weitere Blocke der Datei ausser dem ersten (der eh gelockt ist)
			// gelesen werden muessen, dann erstmal den Block fixen
			if (blockNo != rootBlockNo)
				bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_SHARED));

			// rufe genFirstLast-Methode dieses Objektes auf,
			// um first_ und last_ Attribute dieses Objektes zu befuellen
			// damit man nun weiss, welchen Wert der erste / letzte Value des Blockes hat
			genFirstLast(bacbStack.top().getDataPtr());
			LOG4CXX_DEBUG(logger,"first:"+first().toString());
			LOG4CXX_DEBUG(logger,"last:"+last().toString());

			// ermittle wie mit der binaeren Suche weiter gemacht werden soll
			// Wert ist auf einem Block kleiner als der aktuelle
			// --> gesuchter Block muss vor dem jetzigen liegen
			if (val < first()) {
				lastB = blockNo;
				// der Wert ist auf einem Block nach dem aktuellen
			} else if (val > last()) {
				firstB = blockNo + 1;

				// wenn nicht vor dem aktuellen und nicht danach dann KANN
				// (wenn ueberhaupt vorhanden) der Wert nur im aktuellen Block sein
			} else if (unique) {
				found = true;

				// wenn erster Wert gleich dem gesuchten Wert
			} else if (val == first()) {
				// wenn auf der ersten Seite dann ist der gefundene Wert einfach der gesuchte
				if (blockNo == rootBlockNo) {
					found = true;
					// wenn nicht, dann steht im Block vor diesem Block wahrscheinlich
					// auch schon dergesuchte Wert
					// es sind also mehrere Blocke mit dem Wert gefunden worden
					// diese Zuweisung geht nur, wenn ein Value sich nicht sogar
					// ueber 3 oder noch mehr bloecke verteilen koennte (entriesPerPage: 85)
				} else {
					firstB = blockNo - 1;
					lastB = blockNo;
					foundMulti = true;
					blockMulti = blockNo;
				}
			} else {

				// ansonsten kann der Wert auch hoechsten auf der aktuellen Seite sein
				found = true;
			}

			// wenn eine Seite nach der ersten gesperrt wurde - diese Sperre wieder aufheben
			if (blockNo != rootBlockNo) {
				bufMgr.unfixBlock(bacbStack.top());
				bacbStack.pop();
			}
		}
	}

	// wenn mehrere Blocke gefunden - auch den found Wert fuer einen Block auf true
	if (found == false && foundMulti == true) {
		found = true;
		blockNo = blockMulti;
	}

	// am Ende darf wieder nur ein Block gesperrt sein
	assert(bacbStack.size()==1);

	LOG4CXX_DEBUG(logger,"found: "+TO_STR(found));
	LOG4CXX_DEBUG(logger,"blockNo: "+TO_STR(blockNo));
	return found;
}

/**
 * Durchsucht eine bestimmte Seite nach einem Wert
 * Kein Rueckgabewert, aber es wird die uebergebene Liste tids gefuellt
 */
void DBSeqIndex::findFromPage(const DBAttrType & val, BlockNo blockNo, list<TID> & tids) {
	LOG4CXX_INFO(logger,"findFromPage()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));

	// genau eine Seite gesperrt
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// von Seite mit dem ersten zum Value passenden Tupel suche ueber die
	// folgenden Seite bis Values groesser sind als der jetzige
	bool done = false;
	while (done == false && blockNo < bufMgr.getBlockCnt(file)) {
		// wenn eine andere Seite als die erste angefasst wird - sperren der Seite
		if (blockNo != rootBlockNo)
			bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_SHARED));

		// erstelle Pointer zum Beginn der jetzigen Seite
		const char * ptr = bacbStack.top().getDataPtr();
		// An erster Stelle steht ein uint der die erste FREIE Stelle in der Seite angibt
		uint cnt = *(uint*) ptr;
		// Pointer, um die Laenge des ersten Wertes vorruecken
		ptr += sizeof(uint);
		// wenn wert==0, dann bedeutet das, dass die erste freie Stelle die Nullte ist,
		// alse kein Tupel in der Page ist -> invalid index
		if (cnt == 0)
			throw DBIndexException("Invalid Index Page");

		// iteriere ueber alle gefuellten Tupelfelder in der Seite
		// (cnt ist Stelle des ersten freien Tupels)
		for (uint i = 0; done == false && i < cnt; ++i) {
			// Lese Attributwert an derzeitiger Stelle der Seite
			DBAttrType * attr = DBAttrType::read(ptr, attrType, &ptr);
			// wenn value gefunden
			if (*attr == val) {
				TID * tidList = (TID*) ptr;
				// dann gehe ueber alle TIDs zu dem Value und fuere sie in die "Return"-Liste ein
				for (uint c = 0; c < tidsPerEntry; ++c) {
					if (tidList[c] == invalidTID) {
						break;
					}
					LOG4CXX_DEBUG(logger,"append tid: "+tidList[c].toString());
					tids.push_back(tidList[c]);
				}

				// wenn bereits an gesuchtem Wert vorbei - WHILE SCHLEIFE UNTERBRECHEN
			} else if (*attr > val) {
				done = true;
			}
			// attr pointer loeschen
			delete attr;

			// setze den Pointer, um die Groesse eines Values + TIDs Abschnittes vor
			ptr += sizeof(TID) * tidsPerEntry;
		}

		// eine Seite ungleich der ersten wieder entsperren
		if (blockNo != rootBlockNo) {
			bufMgr.unfixBlock(bacbStack.top());
			bacbStack.pop();
		}
		// ein Block weiter
		++blockNo;
	}

	assert(bacbStack.size()==1);
}

/**
 * Loescht alle Tupel mit den uebergebenen TIDs
 * angefangen auf der Seite mit Blocknummer blockNo
 */
void DBSeqIndex::removeFromPage(const DBAttrType & val, list<TID> tids, BlockNo blockNo) {
	LOG4CXX_INFO(logger,"removeFromPage()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));
	LOG4CXX_DEBUG(logger,"blockNo: "+ TO_STR(blockNo));

	// vor Beginn dieser Operation darf nur genau eine Seite gelockt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// diese eine Seite ist wie immer die erste Seite der Datei - sie erhaelt nun einen Write Lock
	if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
		bufMgr.upgradeToExclusive(bacbStack.top());

	// Liste der tids sortieren
	tids.sort();
	// doppelte Objekte in der Liste entfernen
	tids.unique();
	// Anzahl der TID-Objekte in der Liste ermitteln
	uint cntTIDs = tids.size();

	// Durchlauf der Schleife bis blockNummer
	// (der Block bei dem wir mit dem Entfernen anfangen)
	// bis zur Gesamtseitenzahl der Datei hochgezaehlt ist
	bool done = false;
	while (done == false && cntTIDs != 0 && blockNo < bufMgr.getBlockCnt(file)) {
		// wenn der aktuelle Block nicht der eine erste gesperrte ist, dann exclusive sperren
		if (blockNo != rootBlockNo)
			bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_EXCLUSIVE));

		// erstelle Pointer zum Beginn der jetzigen Seite
		const char * ptr = bacbStack.top().getDataPtr();
		// An erster Stelle steht ein uint der die erste FREIE Stelle in der Seite angibt
		// - in cnt speichern
		uint * cnt = (uint*) ptr;
		// Pointer, um die Laenge des ersten Wertes vorruecken
		ptr += sizeof(uint);
		// wenn wert==0, dann bedeutet das, dass die erste freie Stelle die Nullte ist,
		// alse kein Tupel in der Page ist -> invalid index
		if (*cnt == 0)
			throw DBIndexException("Invalid Index Page");

		// von der ersten stelle im Index bis zur vor die erste freie Stelle durchlaufen,
		// solange ueberhaupt noch TIDs zum Loeschen vorhanden sind
		uint i = 0;
		while (i < *cnt && done == false && cntTIDs != 0) {
			// newPointer berechnen - der auf die naechste TID springt
			const char * nptr;
			// aber erstemal attrValue ermitteln
			DBAttrType * attr = DBAttrType::read(ptr, attrType, &nptr);
			// Zeiger auf ein TID
			TID * tidList = (TID*) nptr;
			nptr += sizeof(TID) * (tidsPerEntry);

			// wenn TID mit val gefunden
			if (*attr == val) {
				uint c = 0;
				// dann gehe alle tids alle Tids per entry durch (zu jedem val koennen MAX_TID_PER_ENTRY TIDS gehoeren, wenn nicht unique)
				while (c < tidsPerEntry && cntTIDs != 0) {
					// wenn der gerade durchlafende TID, invalid ist (also der erste Leere), dann break
					if (tidList[c] == invalidTID) {
						break;
						// wenn der gerade durchlafende TID in der Liste der uebergebenen ist
					} else if (binary_search(tids.begin(), tids.end(), tidList[c])) {
						// dann den Count der uebergebenen Tids um eins minimieren
						--cntTIDs;
						// neues modified date fuer die current page
						bacbStack.top().setModified();
						// alle Tids eins nach vorne kopieren, und die Laufvariable c nicht erhoehen
						memmove(&tidList[c], &tidList[c + 1], sizeof(TID) * (tidsPerEntry - c - 1));
						// den letzten nun mit sicherheit leeren Tid platz mit Invalid TID belegen
						tidList[tidsPerEntry - 1] = invalidTID;
					} else {
						// wenn nicht gefunden und ende der liste noch nicht erreicht dann einen weiter gehen
						++c;
					}
				}
				// wenn beim entfernen der einzige vorhandene Tid zu einem Value entfernt wurde,
				// dann den ganzen Rest der Seite um eine Stelle nach vorne kopieren und so dieses Feld ueerschreiben
				if (tidList[0] == invalidTID) {
					--*cnt;
					memmove((char*) ptr, nptr, (attrTypeSize + sizeof(TID) * (tidsPerEntry)) * (*cnt - i));

					// ansonsten nichts passiert (was komisch ist, da *attr == val), nachstes Feld schauen
				} else {
					++i;
					ptr = nptr;
				}
				// wenn der value bereits hoeher ist, dann abbrechen
			} else if (*attr > val) {
				done = true;

				// ansonsten auch naechstes Feld anschauen
			} else {
				ptr = nptr;
				++i;
			}
			// speicher leeren
			delete attr;
		}

		// wenn nun der erste Wert auf der Seite - der die Anzahl der verbliebenen Tids darstellt, gleich 0 ist, dann kann die ganze Seite geloescht werden
		bool remove = *cnt == 0 ? true : false;

		// jetzige Seite wieder entsperren - wenn nicht die erste
		if (bacbStack.top().getBlockNo() != rootBlockNo) {
			bufMgr.unfixBlock(bacbStack.top());
			bacbStack.pop();
		}

		// schauen ob block geloscht werden soll
		if (remove == true && bufMgr.getBlockCnt(file) > 1) {
			removeEmptyPage(blockNo);
			// oder im naechsten durchgang einfach der nachste Block untersucht werden soll
		} else {
			++blockNo;
		}
	} // while loop solange wie: blockNo < bufMgr.getBlockCnt(file)

	// em ende darf nur noch eine Seite gesperrt sein
	assert(bacbStack.size()==1);

	// und alle Tupel mit den uebergebenen TIDs sollten geloscht sein
	if (cntTIDs > 0)
		throw DBIndexException("could not remove all tids");
}

/**
 * Einfuegen eines neuen (aber moeglicherweise bereits vorhandenen) Wertes und
 * der dazugehoerige TID in eine Seite
 * Start bei Seite mit blockNo
 */
void DBSeqIndex::insertInPage(const DBAttrType & val, const TID & tid, BlockNo blockNo) {
	LOG4CXX_INFO(logger,"insertInPage()");
	LOG4CXX_DEBUG(logger,"val:\n"+val.toString("\t"));
	LOG4CXX_DEBUG(logger,"tid: "+tid.toString());
	LOG4CXX_DEBUG(logger,"blockNo: "+ TO_STR(blockNo));

	// vor beginn dieser operation darf nur genau eine Seite geloggt sein
	if (bacbStack.size() != 1)
		throw DBIndexException("BACB Stack is invalid");

	// diese eine Seite ist wie immer die erste Seite der Datei - sie erhaelt nun einen Write Lock
	if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
		bufMgr.upgradeToExclusive(bacbStack.top());

	bool done = false;

	// bool zum speichern ob ueberhaupt platz ist fuer meinen Wert -> moeglicherweite muss neue Seite eingefuegt werden
	bool insert = false;

	// solange die schleife durchlaufen wie blockNummer (der Block wo wir anfangen mit Einfuegen) noch nicht bis zur Gesamtseitenzahl der Datei hochgezaehlt ist
	while (done == false && blockNo < bufMgr.getBlockCnt(file)) {
		// wenn der aktuelle Block nicht der eine erste gesperrte ist, dann exclusive sperren
		LOG4CXX_DEBUG(logger,"blockNo: "+ TO_STR(blockNo));
		if (blockNo != rootBlockNo)
			bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_EXCLUSIVE));

		// erstelle pointer zum Beginn der jetzigen Seite
		const char * ptr = bacbStack.top().getDataPtr();
		// An erster Stelle steht ein uint der die erste FREIE Stelle in der Seite angibt - in cnt speichern (siehe HubDB.pdf Seite 14)
		uint * cnt = (uint*) ptr;
		// pointer um die laenge des ersten Wertes vorruecken
		ptr += sizeof(uint);
	
		// da ja der erste Wert die erste freie Stelle speichert, die stelle schonmal in pos merken
		uint pos = *cnt;
		LOG4CXX_DEBUG(logger,"cnt: "+ TO_STR(*cnt));
		LOG4CXX_DEBUG(logger,"pos: "+ TO_STR(pos));

		// nun von der ersten bis zur ersten freien Stelle, alle Felder mit TID objekten durchlaufen
		for (uint i = 0; i < *cnt && done == false; ++i) {
			// newPointer berechnen - der auf die naechste TID springt
			const char * nptr;
			// aber erstemal attrValue ermitteln
			DBAttrType * attr = DBAttrType::read(ptr, attrType, &nptr);
			// Zeiger auf ein TID
			TID * tidList = (TID*) nptr;
			nptr += sizeof(TID) * (tidsPerEntry);
			LOG4CXX_DEBUG(logger,"attr:\n"+ attr->toString("\t"));

			// wenn value gefunden
			if (val == *attr) {
				LOG4CXX_DEBUG(logger,"val == attr");
				// bereits den key gefunden der jetzt eingefuegt werden soll UND Unique fuehrt zu Exception
				if (unique == true)
					throw DBIndexUniqueKeyException("Attr already in index");

				// von aktueller Stelle weitere tidsPerEntry
				for (uint c = 0; c < tidsPerEntry; ++c) {
					// gehe bis an die stelle in der TID liste wo nich nichts steht
					if (tidList[c] == invalidTID) {
						LOG4CXX_DEBUG(logger,"invalid TID: " + TO_STR(c));
						// wenn noch nix eingefuegt dann genau hier die neue TID einfuegen - ZUM BEREITS VORHANDENEN WERT
						if (insert == false) {
							bacbStack.top().setModified();
							tidList[c] = tid;
							// wenn noch nicht an der letzten Stelle eingefuegt, dann auf die naechste Stelle invalidTID setzten
							if ((c + 1) < tidsPerEntry) {
								tidList[c + 1] = invalidTID;
							}

							// PLatz vorhanden, insert kein problem
							insert = true;
						}
						// man brauch auch die TIDs nicht weiter durchschauen
						break;

						// natuerlich nur TID einfuegen wenn noch nicht vorhanden
						// dieser else block wird fuer alle c kleiner als das letzte c wo invalidTID steht, durchlaufen
					} else if (tidList[c] == tid) {
						throw DBIndexUniqueKeyException("Attr,tid already in index");
					}
				}

				// wenn einzufuegender Wert kleiner als der aktuelle durchlaufende Wert, pos auf die Stelle setzen wo value spaeter rein soll
			} else if (val < *attr) {
				LOG4CXX_DEBUG(logger,"val < attr");
				pos = i;
				done = true;
			}

			// clear fuer neuen durchlauf
			delete attr;
			ptr = nptr;
		} // for

		LOG4CXX_DEBUG(logger,"pos: "+ TO_STR(pos));
		LOG4CXX_DEBUG(logger,"insert: "+ TO_STR(insert));
		LOG4CXX_DEBUG(logger,"done: "+ TO_STR(done));

		// wenn TID bei vorhanden value nicht eingefuegt
		if (insert == false) {
			bool unfix = false;

			// wenn nichts eingefugt weil alle Stellen auf der Seite ohne erfolg durchlaufen, dann
			if (*cnt == entriesPerPage()) {
				LOG4CXX_DEBUG(logger,"split page cnt: " + TO_STR(*cnt));

				// split page
				// setze prtOld auf eine Stelle nach der Haelfte der Seite um alle Values auf der Seite durch 2 zu teilen
				// auf block anfang
				const char * ptrOld = bacbStack.top().getDataPtr();
				// um den ersten uint (stelle mit erstem freien wert) weiter gehen
				ptrOld += sizeof(uint);
				// halbe entry menge berechnen
				*cnt = (entriesPerPage() - entriesPerPage() / 2);
				LOG4CXX_DEBUG(logger,"leftcnt: " + TO_STR(*cnt));
				// um die halbe entry menge in benoetigten Byte weitergehen
				ptrOld += (sizeof(TID) * tidsPerEntry + attrTypeSize) * (*cnt);
				bacbStack.top().setModified();

				// neue LEERE Seite einfuegen + locken + modify date setzen
				insertEmptyPage(bacbStack.top().getBlockNo() + 1);
				bacbStack.push(bufMgr.fixBlock(file, bacbStack.top().getBlockNo() + 1, LOCK_EXCLUSIVE));
				bacbStack.top().setModified();

				// pointer auf anfang der neuen blockes bekommen
				char * ptrNew = bacbStack.top().getDataPtr();
				// pointer auf den ersten uint speichern
				uint * cntNew = (uint*) ptrNew;
				// pointer um diesen ersten uint weiter ruecken
				ptrNew += sizeof(uint);

				// anzahl der zukuenftigen Anzahl von Values auf der neuen Seite ausrechnen (gesamtanzahl minus Anzahl nach der geteilt wird auf der alten seite)
				*cntNew = entriesPerPage() - *cnt;
				LOG4CXX_DEBUG(logger,"rightcnt: " + TO_STR(*cntNew));
				// cntNew bytes von der alten Seite auf die neue Seite kopieren (also allvalue samt TIDs nach det Teilposition)
				// memcpy (*destination, *source, size);
				memcpy(ptrNew, ptrOld, (sizeof(TID) * tidsPerEntry + attrTypeSize) * (*cntNew));

				// do not check block twice
				++blockNo;
				// wenn die Stelle bei der geteilt wurde groesser ist, als die Stelle bei der ich einfuegen will,
				// dann kann der recht lock schonmal unfixed werden... alles weitere im linken block
				if (pos <= *cnt) {
					LOG4CXX_DEBUG(logger,"unfix right page");
					bufMgr.unfixBlock(bacbStack.top());
					bacbStack.pop();

					// ansonsten liegt die Stelle auf der neuen Seite (jetzt natuerlich um die Anzahl der Werte die auf der alten Seiten geblieben sind minimiert)
				} else {
					pos -= *cnt;
					unfix = true;
					LOG4CXX_DEBUG(logger,"new pos: "+ TO_STR(pos));
				}
			}
			// nun ist platz da, insert kein Problem mehr
			insert = true;

			// stelle erhalten wo einfuegen moeglich ist
			uint * cntX = (uint*) bacbStack.top().getDataPtr();
			LOG4CXX_DEBUG(logger,"pos: "+ TO_STR(pos));
			LOG4CXX_DEBUG(logger,"cntX: "+ TO_STR(cntX)+" "+ TO_STR(*cntX));
			// Byte Stelle erhalten wo ich gerne einfuegen moechte (variable pos)
			char * from = bacbStack.top().getDataPtr() + sizeof(uint) + (sizeof(TID) * tidsPerEntry + attrTypeSize) * pos;
			LOG4CXX_DEBUG(logger,"from: "+ TO_STR(from));
			// wenn dort wo einfuegen moeglich und dort wo ich einfuegen moechte nicht gleich sind...
			if (*cntX != pos) {
				// Byte stelle einen Value nach der stelle wo ich gerne einfuegen moechte berechnen
				char * to = from + (sizeof(TID) * tidsPerEntry + attrTypeSize);
				LOG4CXX_DEBUG(logger,"to: "+ TO_STR(to));

				// alles bis zum ersten freien Value nehmen und um eine Stelle weiter schieben, um so an pos platz zu schaffen
				memmove(to, from, (sizeof(TID) * tidsPerEntry + attrTypeSize) * (*cntX - pos));
			}
			LOG4CXX_DEBUG(logger,"val: "+ val.toString("\t"));
			// an Btye stelle from (quasi integer stelle pos) nun mein val einfuegen
			from = val.write(from);
			// und die zu speichernde TID gleich hinterher
			TID * tidList = (TID*) from;
			LOG4CXX_DEBUG(logger,"tidList: "+ TO_STR(tidList));
			tidList[0] = tid;

			// wenn mehrere TIDs moeglich, dann die invalidTID hintendran haengen
			if (!unique)
				tidList[1] = invalidTID;

			// am anfang des blockes den Wert wo der naechste freie Platz ist um eins erhoehen
			++*cntX;
			LOG4CXX_DEBUG(logger,"cntX: "+ TO_STR(*cntX));

			// neues modified date
			bacbStack.top().setModified();

			// unfixen!
			if (unfix == true) {
				bufMgr.unfixBlock(bacbStack.top());
				bacbStack.pop();
			}

			if (unique == true)
				done = true;
		} // if insert


		// wieder nur die erste Seite gelockt am Ende der ganzen Methode
		if (bacbStack.top().getBlockNo() != rootBlockNo) {
			bufMgr.unfixBlock(bacbStack.top());
			bacbStack.pop();
		}
		// und ein Block weiter schauen
		++blockNo;
	} // while

	// nochmal sicher gehen das wirklich genau ein Block gelockt
	assert(bacbStack.size()==1);
}

/**
 * Diese Methode fuegt in die sequenzielle Indexdatei (* file)
 * einen Block an Position pos ein
 */
void DBSeqIndex::insertEmptyPage(BlockNo pos) {
	LOG4CXX_INFO(logger,"insertEmptyPage()");
	LOG4CXX_DEBUG(logger,"blockNo: "+ TO_STR(pos));

	// anzahl der geblockten seiten
	uint s = bacbStack.size();

	// die oberste Seite des Stacks nun EXCLUSIV blocken
	if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
		bufMgr.upgradeToExclusive(bacbStack.top());

	// neue Page am ende von file blocken
	bacbStack.push(bufMgr.fixNewBlock(file));
	// nummer der neuen page ermitteln
	BlockNo blockNo = bacbStack.top().getBlockNo();

	// so lange vom ende der page list (von nummer der neuen seite) runterzahlen bis bei der aktuellen Position angelangt
	while (blockNo > pos) {
		// im ersten Durchlauf...
		--blockNo;
		// ...der Variable 'right' die neue leere Seite zuweisen, die aktuell ganz oben auf dem Stack liegt
		DBBACB right = bacbStack.top();
		// ...die Seite links der neuen (blockNummer der neuen Seite - 1), also die vor dem anhaengen letze Seite sperren
		bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_EXCLUSIVE));
		// ...der variable 'left' die gerade gesperrte Seite zuweisen (letzte gefuellte Seite)
		DBBACB left = bacbStack.top();
		// ... dann die ganze Seite Left in die Seite 'Right' kopieren (void * memcpy ( void * destination, const void * source, size_t num );)
		memcpy(right.getDataPtr(), left.getDataPtr(), DBFileBlock::getBlockSize());
		// ... 'right' bekommt nun ein neues modified date
		right.setModified();
		// ... links entsperren
		bacbStack.pop();
		// ... rechts entsperren wenn nicht erste Seite
		if (bacbStack.top().getBlockNo() != rootBlockNo) {
			bufMgr.unfixBlock(bacbStack.top());
			bacbStack.pop();
		}
		// ... links erneut sperren
		bacbStack.push(left);
	}
	// nachdem alle seite von pos bis zum ende (wo jetzt die neue Seite ist) eine position weiter geschoben wurden, muss die Seite bei pos geloscht werden
	memset(bacbStack.top().getDataPtr(), 0, DBFileBlock::getBlockSize());
	// die pos seite bekommt nun auch neues Modified date
	bacbStack.top().setModified();

	// wenn die pos Seite nicht die rootSeite ist, dann unlocken
	if (bacbStack.top().getBlockNo() != rootBlockNo) {
		bufMgr.unfixBlock(bacbStack.top());
		bacbStack.pop();
	}

	// am ende der ganzen operation muessen wieder genauso viele Blocke gelockt sein, wie am Anfang
	if (bacbStack.size() != s)
		throw DBIndexException("BACB Stack is invalid");
}

/**
 * Loescht an Position pos eine leere Seite
 */
void DBSeqIndex::removeEmptyPage(BlockNo blockNo) {
	LOG4CXX_INFO(logger,"removeEmptyPage()");
	LOG4CXX_DEBUG(logger,"blockNo: "+ TO_STR(blockNo));

	// Hoehe des Stappels der gesperrten Seiten vor dem loeschen
	uint s = bacbStack.size();
	// anzahl der Seiten
	uint blockCnt = bufMgr.getBlockCnt(file);

	// wenn mehr als eine Seite in Datei vorhanden
	if (blockCnt > 1) {
		// wenn die leere seite nicht die eh schon gesperrte erste Seite ist, dann EXCLUSIVE Locken
		if (blockNo != rootBlockNo)
			bacbStack.push(bufMgr.fixBlock(file, blockNo, LOCK_EXCLUSIVE));

		// von der zum loeschenden Seite + 1 bis zur Gesamtanzahl der Seiten hochzahlen
		for (BlockNo i = blockNo + 1; i < blockCnt; ++i) {
			// und alle seiten eine Stelle nach links verschieben und wahrend der operation locken
			DBBACB left = bacbStack.top();
			bacbStack.push(bufMgr.fixBlock(file, i, LOCK_EXCLUSIVE));
			DBBACB right = bacbStack.top();
			memcpy(left.getDataPtr(), right.getDataPtr(), DBFileBlock::getBlockSize());
			left.setModified();
			bacbStack.pop();
			if (bacbStack.top().getBlockNo() != rootBlockNo) {
				bufMgr.unfixBlock(bacbStack.top());
				bacbStack.pop();
			}
			bacbStack.push(right);
		}

		// nach der Operation die zu loeschende wieder freigeben
		if (bacbStack.top().getBlockNo() != rootBlockNo) {
			bufMgr.unfixBlock(bacbStack.top());
			bacbStack.pop();
		}

		// gesamtseitenzahl um eins veringern und so eine seite am ende "abschneiden"
		bufMgr.setBlockCnt(file, blockCnt - 1);
	}

	// die anzahl der gesperrten Seiten muss gleich geblieben sein
	if (bacbStack.size() != s)
		throw DBIndexException("BACB Stack is invalid");
}

/**
 * Prueft, ob eine Seite Tupel enthalt.
 * Es wird der uint an der nullten Stelle eines Blockes derefernziert
 */
bool DBSeqIndex::isEmpty(const char * ptr) {
	uint cnt = *(uint*) ptr;
	if (cnt == 0)
		return true;
	return false;
}

/**
 * Erstes und letztes Element eines Blockes in die Variablen first_ und last_ abspeichern
 */
void DBSeqIndex::genFirstLast(const char * ptr) {
	//  INFO [0xb7854b70] {DBSeqIndex.cpp:567} Index::DBSeqIndex::genFirstLast - genFirstLast()
	LOG4CXX_INFO(logger,"genFirstLast()");
	// wenn first_ bereits belegt -> loeschen
	if (first_ != NULL)
		delete first_;
	// wenn last_ bereits belegt -> loeschen
	if (last_ != NULL)
		delete last_;
	// erstelle Zeiger auf ... vom uebergebenen pointer
	uint * cnt = (uint*) ptr;
	ptr += sizeof(uint);
	// wenn dereferenzierter Wert 0 ist -> dann ist da nix mehr -> throw Exception
	if (*cnt == 0)
		throw DBIndexException("Invalid Index Page");
	// fuelle first attribut
	first_ = DBAttrType::read(ptr, attrType);
	// errechne stelle und dann fuelle last attribut
	ptr += (sizeof(TID) * tidsPerEntry + attrTypeSize) * (*cnt - 1);
	last_ = DBAttrType::read(ptr, attrType);
}

/**
 * Fuegt createDBSeqIndex zur globalen factory method-map hinzu
 */
int DBSeqIndex::registerClass() {
	setClassForName("DBSeqIndex", createDBSeqIndex);
	return 0;
}

/**
 * Gerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 * - DBBufferMgr *: Buffermanager
 * - DBFile *: Dateiobjekt
 * - attrType: Attributtp
 * - ModeType: READ, WRITE
 * - bool: unique Indexattribut
 */
extern "C" void * createDBSeqIndex(int nArgs, va_list & ap) {
	// Genau 5 Parameter
	if (nArgs != 5) {
		throw DBException("Invalid number of arguments");
	}
	DBBufferMgr * bufMgr = va_arg(ap,DBBufferMgr *);
	DBFile * file = va_arg(ap,DBFile *);
	enum AttrTypeEnum attrType = (enum AttrTypeEnum) va_arg(ap,int);
	ModType m = (ModType) va_arg(ap,int);
	bool unique = (bool) va_arg(ap,int);
	return new DBSeqIndex(*bufMgr, *file, attrType, m, unique);
}
