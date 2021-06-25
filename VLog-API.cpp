#include <jni.h>

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>
#include <vlog/cycles/checker.h>
#include <vlog/reasoner.h>
#include <vlog/utils.h>
#include <kognac/utils.h>
#include <kognac/logs.h>

#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>

#define IS_BLANK(c) (c >= (INT64_C(1) << 40))

class VLogInfo {
	public:
		SemiNaiver *sn;
		Program *program;
		EDBLayer *layer;

		VLogInfo() {
			sn = NULL;
			program = NULL;
			layer = NULL;
		}

		~VLogInfo() {
			if (layer != NULL) {
				delete layer;
				layer = NULL;
			}
			if (program != NULL) {
				delete program;
				program = NULL;
			}
			if (sn != NULL) {
				delete sn;
				sn = NULL;
			}
		}
};

static std::map<int, VLogInfo *> vlogMap;

static bool logLevelSet = false;

// Utility method to convert a literal id to a string.
std::string literalToString(VLogInfo *f, uint64_t literalid) {

	std::string s = f->layer->getDictText(literalid);

	if (s == std::string("")) {
		s = "" + std::to_string(literalid >> 40) + "_"
			+ std::to_string((literalid >> 32) & 0377) + "_"
			+ std::to_string(literalid & 0xffffffff);
	}

	return s;
}

VLogInfo *getVLogInfo(int id) {
	auto inf = vlogMap.find(id);
	if (inf == vlogMap.end()) {
		return NULL;
	}
	return inf->second;
}

// Utility method to throw an exception
void throwException( const char *message) {
    //TODO Irina: throw std::exception/ std::runtime_error ?
	throw std::exception(message);
}

// Converts a vector of Atoms into VLog representation.
std::vector<Literal> getVectorLiteral(JNIEnv *env, VLogInfo *f, jobjectArray h, Dictionary &dict) {
	std::vector<Literal> result;
	jsize sz = env->GetArrayLength(h);
	// For all atoms:
	for (int i = 0; i < sz; i++) {
		// First, get the atom.
		jobject atom = env->GetObjectArrayElement(h, (jsize) i);
		jclass cls = env->GetObjectClass(atom);

		// Get the predicate
		jmethodID getPredicateMethod = env->GetMethodID(cls, "getPredicate", "()Ljava/lang/String;");
		jstring jpred = (jstring) env->CallObjectMethod(atom, getPredicateMethod);
		std::string predicate = jstring2string(env, jpred);

		// Get the terms
		jmethodID getTermsMethod = env->GetMethodID(cls, "getTerms", "()[Lkarmaresearch/vlog/Term;");
		jobjectArray jterms = (jobjectArray) env->CallObjectMethod(atom, getTermsMethod);

//		// Get negated
//		jmethodID isNegated = env->GetMethodID(cls, "isNegated", "()Z");
//		jboolean jnegated = (jboolean) env->CallBooleanMethod(atom, isNegated);
//		bool negated = jnegated == JNI_TRUE;
//		bool negated = false;


		jsize vtuplesz = env->GetArrayLength(jterms);
		// Collect conversions from terms
		if (vtuplesz != (uint8_t) vtuplesz) {
			throwException(("Arity of predicate " + predicate + " too large (" + std::to_string(vtuplesz) + " > 255)").c_str());
			return result;
		}
		VTuple tuple((uint8_t) vtuplesz);
		std::vector<VTerm> t;

		// For each term:
		for (int j = 0; j < vtuplesz; j++) {
			// First, get the term
			jobject jterm = env->GetObjectArrayElement(jterms, (jsize) j);
			jclass termcls = env->GetObjectClass(jterm);
//
// TODO:
//			// Get the name
			jmethodID getNameMethod = env->GetMethodID(termcls, "getName", "()Ljava/lang/String;");
			jstring jname = (jstring) env->CallObjectMethod(jterm, getNameMethod);
			std::string name = jstring2string(env, jname);

			// Get the type: constant or variable
			jmethodID getTypeMethod = env->GetMethodID(termcls, "getTermType", "()Lkarmaresearch/vlog/Term$TermType;");
			jobject jtype = env->CallObjectMethod(jterm, getTypeMethod);
			jclass enumClass = env->GetObjectClass(jtype);
			jmethodID getOrdinalMethod = env->GetMethodID(enumClass, "ordinal", "()I");
			jint type = (jint) env->CallIntMethod(jtype, getOrdinalMethod);

			// For now, we only have two choices.
			if (type != 0) {
				// Variable
				uint64_t v = dict.getOrAdd(name);
				t.push_back(VTerm((uint8_t) v, 0));
			} else {
				// Constant
				// name = program->rewriteRDFOWLConstants(name);
				uint64_t dictTerm;
				f->layer->getOrAddDictNumber(name.c_str(), name.size(), dictTerm);
				t.push_back(VTerm(0, dictTerm));
			}
		}
		int pos = 0;
		for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
			tuple.set(*itr, pos++);
		}

		uint8_t adornment = Predicate::calculateAdornment(tuple);

		int64_t predid = f->program->getOrAddPredicate(predicate, (uint8_t) vtuplesz);
		if (predid < 0) {
			throwException(("wrong cardinality in predicate " + predicate).c_str());
			return result;
		}

		Predicate pred((PredId_t) predid, adornment, f->layer->doesPredExists((PredId_t) predid) ? EDB : IDB, (uint8_t) vtuplesz);

		Literal literal(pred, tuple, false);
		result.push_back(literal);
	}
	return result;
}
#ifdef __cplusplus
extern "C" {
#endif


/*
 * Modified	JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setLogLevel(JNIEnv *env, jobject obj, jobject level) {
 * Class:     karmaresearch_vlog_VLog
 * Method:    setLogLevel
 * Signature: (Lkarmaresearch/vlog/VLog/LogLevel;)V
 */
    void setLogLevel(int level) {
	switch(level) {
		case 0:
			Logger::setMinLevel(ERRORL);
			break;
		case 1:
			Logger::setMinLevel(WARNL);
			break;
		case 2:
			Logger::setMinLevel(INFOL);
			break;
		case 3:
			Logger::setMinLevel(DEBUGL);
			break;
		default:
			Logger::setMinLevel(TRACEL);
			break;
	}
	logLevelSet = true;
}

	/*
	 * Modified 	JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setLogFile(JNIEnv *env, jobject obj, jstring file)
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    setLogFile
	 * Signature: (Ljava/lang/String;)V
	 */
	 void setLogFile(std::string file) {
	     // TODO Irina NULL equivalent in C++
		if (file == NULL) {
			Logger::logToFile("");
			return;
		}
		Logger::logToFile(file);
	}

	/*
	 *  Modified: 	JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_start(JNIEnv *env, jobject obj, jstring rawconf, jboolean isfile)
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    start
	 * Signature: (Ljava/lang/String;Z)V
	 */
	 void start(int vLogId, std::string crawconf) {
		VLogInfo *f = getVLogInfo(vLogId);
		if (f != NULL) {
            throwException("VLog is already started");
			return;
		}

		if (! logLevelSet) {
			Logger::setMinLevel(INFOL);
		}
		f = new VLogInfo();
		try {
			EDBConf conf(crawconf.c_str(), false);

			try {
				f->layer = new EDBLayer(conf, false);
			} catch(std::string s) {
                 throwException(s.c_str());
				return;
			}
			f->program = new Program(f->layer);
		} catch(std::string s) {
             throwException(s.c_str());
			return;
		} catch(char const *s) {
			 throwException(s);
			return;
		}
		vlogMap[vLogId] = f;
	}

	/*
	 * Modified JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_stop(JNIEnv *env, jobject obj)
	 *
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    stop
	 * Signature: ()V
	 */
	 void stop(int vLogId) {
		auto inf = vlogMap.find(vLogId);
		if (inf == vlogMap.end()) {
			return;
		}
		delete inf->second;
		vlogMap.erase(inf);
	}

	/*
	 * Modified 	JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_addData(JNIEnv *env, jobject obj, jstring jpred, jobjectArray data)
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    addData
	 * Signature: (Ljava/lang/String;[[Ljava/lang/String;)V
	 */
	 void addData(JNIEnv *env, int vLogId, std::string pred, jobjectArray data) {
	     //TODO Irina, continue modifying, get rid of env
		VLogInfo *f = getVLogInfo(vLogId);
		if (f == NULL) {
			f = new VLogInfo();
			vlogMap[vLogId] = f;
		}

		if (! logLevelSet) {
			Logger::setMinLevel(INFOL);
		}

		if (f->layer == NULL) {
			EDBConf conf("", false);
			f->layer = new EDBLayer(conf, false);
		}

		if (f->program != NULL) {
			if (f->program->getNRules() > 0) {
				throwException("Cannot add data if there already are rules");
				return;
			}
			delete f->program;
			f->program = NULL;
		}

		if (data == NULL) {
			throwException("null data");
			return;
		}
		jsize nrows = env->GetArrayLength(data);
		// For all rows:
		std::vector<std::vector<std::string>> values;
		for (int i = 0; i < nrows; i++) {
			// First, get the atom.
			std::vector<std::string> value;
			jobjectArray atom = (jobjectArray) env->GetObjectArrayElement(data, (jsize) i);
			if (atom == NULL) {
				// throwEDBConfigurationException(env, "null data");
				// return;
				continue;	// For issue #25
			}
			jint arity = env->GetArrayLength(atom);
			if (arity != (uint8_t) arity) {
				throwException( ("Arity of " + pred + " too large (" + std::to_string(arity) + " > 255)").c_str());
				return;
			}
			for (int j = 0; j < arity; j++) {
				jstring v = (jstring) env->GetObjectArrayElement(atom, (jsize) j);
				if (v == NULL) {
					throwException("null data");
					return;
				}
				value.push_back(jstring2string(env, v));
			}
			values.push_back(value);
		}

		try {
			f->layer->addInmemoryTable(pred, values);
		} catch(std::string s) {
			throwException(s.c_str());
			return;
		} catch(char const *s) {
			throwException(s);
			return;
		}

		f->program = new Program(f->layer);
	}


	/*
	 * Modified 	JNIEXPORT jint JNICALL Java_karmaresearch_vlog_VLog_getPredicateId(JNIEnv *env, jobject obj, jstring p)
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getPredicateId
	 * Signature: (Ljava/lang/String;)I
	 */
	 int getPredicateId(VLogInfo *f, std::string predName) {

		if (f == NULL || f->program == NULL) {
			throwException( "VLog is not started yet");
			return -1;
		}

		// This might create a new predicate if it does not exist.
		// There should be a way to just do a lookup???
		Predicate pred = f->program->getPredicate(predName);
		if (pred.getCardinality() == 0) {
			// Does not exist yet.
			return -1;
		}

		return pred.getId();
	}

	/*
	 * Modified 	JNIEXPORT jint JNICALL Java_karmaresearch_vlog_VLog_getPredicateArity(JNIEnv *env, jobject obj, jstring p)
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getPredicateArity
	 * Signature: (Ljava/lang/String;)I
	 */
	 int getPredicateArity(VLogInfo *f, std::string predName ) {

		if (f == NULL || f->program == NULL) {
			throwNotStartedException(env, "VLog is not started yet");
			return -1;
		}

		// TODO: fix this: this might create a new predicate if it does not exist.
		// There should be a way to just do a lookup???
		Predicate pred = f->program->getPredicate(predName);
		if (pred.getCardinality() == 0) {
			return  -1;
		}
		return pred.getCardinality();
	}

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getPredicate
	 * Signature: (I)Ljava/lang/String;
	 */
	JNIEXPORT jstring JNICALL Java_karmaresearch_vlog_VLog_getPredicate(JNIEnv *env, jobject obj, jint predid) {
		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->program == NULL) {
			throwException("VLog is not started yet");
			return NULL;
		}

		std::string pred = f->program->getPredicateName((PredId_t) predid);
		if (pred == std::string("")) {
			return NULL;
		}
		return env->NewStringUTF(pred.c_str());
	}

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getConstantId
	 * Signature: (Ljava/lang/String;)J
	 */
	JNIEXPORT jlong JNICALL Java_karmaresearch_vlog_VLog_getConstantId(JNIEnv *env, jobject obj, jstring literal) {

		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->layer == NULL) {
			throwException("VLog is not started yet");
			return (jint) -1;
		}

		const char *cliteral = env->GetStringUTFChars(literal, 0);
		uint64_t value;
		jlong retval = -1;
		if (f->layer->getDictNumber(cliteral, strlen(cliteral), value)) {
			retval = value;
		}
		env->ReleaseStringUTFChars(literal, cliteral);
		return retval;
	}

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getOrAddConstantId
	 * Signature: (Ljava/lang/String;)J
	 */
	JNIEXPORT jlong JNICALL Java_karmaresearch_vlog_VLog_getOrAddConstantId(JNIEnv *env, jobject obj, jstring literal) {

		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->layer == NULL) {
			throwException("VLog is not started yet");
			return (jint) -1;
		}

		const char *cliteral = env->GetStringUTFChars(literal, 0);
		uint64_t value;
		jlong retval = -1;
		if (f->layer->getOrAddDictNumber(cliteral, strlen(cliteral), value)) {
			retval = value;
		}
		env->ReleaseStringUTFChars(literal, cliteral);
		return retval;
	}

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    getConstant
	 * Signature: (J)Ljava/lang/String;
	 */
	JNIEXPORT jstring JNICALL Java_karmaresearch_vlog_VLog_getConstant(JNIEnv *env, jobject obj, jlong literalid) {
		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->program == NULL) {
			throwException( "VLog is not started yet");
			return NULL;
		}
		std::string s = f->layer->getDictText(literalid);
		if (s == std::string("")) {
			return NULL;
		}
		return env->NewStringUTF(s.c_str());
	}

	static TupleIterator *getQueryIter(JNIEnv *env, jobject obj, PredId_t p, jlongArray els, jboolean includeConstants) {
		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->program == NULL) {
			throwException("VLog is not started yet");
			return NULL;
		}

		// Create a VLog query from the parameters.
		Predicate pred = f->program->getPredicate((PredId_t) p);
		if (pred.getCardinality() == 0) {
			return NULL;
		}
		jsize sz = env->GetArrayLength(els);
		VTuple tuple((uint8_t) sz);
		jlong *e = env->GetLongArrayElements(els, NULL);
		for (int i = 0; i < sz; i++) {
			jlong v = e[i];
			uint8_t varId = 0;
			uint64_t val = 0;
			if (v < 0) {
				varId = (uint8_t) -v;
			} else {
				val = v;
			}
			VTerm vterm(varId, val);
			tuple.set(vterm, i);
		}
		env->ReleaseLongArrayElements(els, e, JNI_ABORT);
		Literal query(pred, tuple);

		// Now create an iterator over the query result.
		TupleIterator *iter = NULL;
		Reasoner r((uint64_t) 0);
		if (pred.getType() == EDB) {
			iter = r.getEDBIterator(query, NULL, NULL, *(f->layer), ! (bool) includeConstants, NULL);
		} else if (f->sn != NULL) {
			iter = r.getIteratorWithMaterialization(f->sn, query, ! (bool) includeConstants, NULL);
		} else {
			// No materialization yet, but non-EDB predicate ... so, empty.
			TupleTable *table = new TupleTable(sz);
			std::shared_ptr<TupleTable> pt(table);
			iter = new TupleTableItr(pt);
		}
		return iter;
	}

    static int numberOfUniqueVars(JNIEnv *env, jlongArray els) {
        std::vector<uint8_t> vars;
        jsize sz = env->GetArrayLength(els);
        jlong *e = env->GetLongArrayElements(els, NULL);
        uint8_t varId;
        for (int i = 0; i < sz; i++) {
            if (e[i] < 0) {
                varId = (uint8_t) -e[i];
                if(std::find(vars.begin(), vars.end(), varId) == vars.end()) {
                    vars.push_back(varId);
                }
            }
        }
        return vars.size();
    }

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    query
	 * Signature: (I[JZZ)Lkarmaresearch/vlog/QueryResultIterator;
	 */
	JNIEXPORT jobject JNICALL Java_karmaresearch_vlog_VLog_query(JNIEnv * env, jobject obj, jint p, jlongArray els, jboolean includeConstants, jboolean filterBlanks) {
		if (p == -1) {
			throwException("Query contains non-existing predicate");
			return NULL;
		}

		TupleIterator *iter = getQueryIter(env, obj, (PredId_t) p, els, includeConstants);
		if (iter == NULL) {
			return NULL;
		}
		jclass jcls=env->FindClass("karmaresearch/vlog/QueryResultIterator");
		jmethodID mID = env->GetMethodID(jcls, "<init>", "(JZ)V");
		jobject jobj = env->NewObject(jcls, mID, (jlong) iter, filterBlanks);

		return jobj;
	}

/*
* Class:     karmaresearch_vlog_VLog
* Method:    nativeQuerySize
* Signature: (I[JZZ)J;
*/
JNIEXPORT jlong JNICALL Java_karmaresearch_vlog_VLog_nativeQuerySize(JNIEnv * env, jobject obj, jint p, jlongArray els, jboolean includeConstants, jboolean filterBlanks) {
    jlong result = 0;
    VLogInfo *f = getVLogInfo(env, obj);
    if (f == NULL || f->program == NULL) {
        throwException( "VLog is not started yet");
        return result;
    }
    if (p == -1) {
        throwException( "Query contains non-existing predicate");
        return result;
    }
    Predicate pred = f->program->getPredicate((PredId_t) p);

    if ((pred.getCardinality() == numberOfUniqueVars(env, els)) && !filterBlanks) {
        if (pred.getType() == EDB) {
            return f->layer->getPredSize(p);
        } else if (f->sn != NULL) {
            return f->sn->getSizeTable(p);
        } else {
            // pred.getType()==IDB and f->sn==NULL
            throwException("Materialization has not run yet");
            return result;
        }
    } else {
        TupleIterator *iter = getQueryIter(env, obj, (PredId_t) p, els, (jboolean) includeConstants);
        if (iter == NULL) {
            return result;
        }
        size_t sz = iter->getTupleSize();
        while (iter->hasNext()) {
            iter->next();
            if (filterBlanks) {
                bool filterOut = false;
                for (int i = 0; i < sz; i++) {
                    if (IS_BLANK(iter->getElementAt(i))) {
                        filterOut = true;
                        break;
                    }
                }
                if (!filterOut) {
                    ++result;
                }
            }
            else {
                ++result;
            }
        }
        return result;
    }
}


	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    setRules
	 * Signature: ([Lkarmaresearch/vlog/Rule;Lkarmaresearch/vlog/VLog/RuleRewriteStrategy;)V
	 */
	JNIEXPORT void JNICALL Java_karmaresearch_vlog_VLog_setRules(JNIEnv *env, jobject obj, jobjectArray rules) {
		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->program == NULL) {
			throwException( "VLog is not started yet");
			return;
		}
		if (rules != NULL) {
			// Create a new program, to remove any left-overs from old rule stuff
			delete f->program;
			f->program = new Program(f->layer);

			// Get number of rules
			jsize sz = env->GetArrayLength(rules);

			// For each rule:
			for (int i = 0; i < sz; i++) {
				Dictionary dictVariables; // temporary dictionary for variables.

				jobject rule = env->GetObjectArrayElement(rules, (jsize) i);
				jclass cls = env->GetObjectClass(rule);
				jmethodID getHeadMethod = env->GetMethodID(cls, "getHead", "()[Lkarmaresearch/vlog/Atom;");
				jmethodID getBodyMethod = env->GetMethodID(cls, "getBody", "()[Lkarmaresearch/vlog/Atom;");

				// Get fields: head and body.
				jobjectArray head = (jobjectArray) env->CallObjectMethod(rule, getHeadMethod);
				jobjectArray body = (jobjectArray) env->CallObjectMethod(rule, getBodyMethod);

				// Convert them into internal VLog format.
				std::vector<Literal> vhead = getVectorLiteral(env, f, head, dictVariables);
				std::vector<Literal> vbody = getVectorLiteral(env, f, body, dictVariables);

				// Test number of variables
				uint64_t v = dictVariables.getOrAdd("___DUMMY___DUMMY___DUMMY");
				if (v > 256) {
					throwException( ("Too many variables in rule " + std::to_string(i)).c_str());
					return;
				}

				for (auto lit : vhead) {
					if (lit.getPredicate().getType() == EDB) {
						std::string pred = f->program->getPredicateName(lit.getPredicate().getId());
						if (pred == std::string("")) {
							throwException( "Rule head cannot contain EDB predicate");
						} else {
							throwIllegalArgumentException(("Rule head cannot contain EDB predicate " + pred).c_str());
						}
						return;
					}
				}

				// And add the rule.
				f->program->addRule(vhead, vbody, false);
			}
		}
	}

	/*
	 * Class:     karmaresearch_vlog_VLog
	 * Method:    materialize
	 * Signature: (ZI)Z
	 */
	JNIEXPORT bool JNICALL Java_karmaresearch_vlog_VLog_materialize(JNIEnv *env, jobject obj) {
		VLogInfo *f = getVLogInfo(env, obj);
		if (f == NULL || f->program == NULL) {
			throwException("VLog is not started yet");
			return false;
		}

		if (f->sn != NULL) {
			delete f->sn;
            f->sn = NULL;
		}

		LOG(INFOL) << "Starting full materialization";
		try {
			f->sn = new SemiNaiver(*(f->layer), f->program, true, false, false,
					 TypeChase::RESTRICTED_CHASE,
					-1, false, false);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			unsigned long *p = NULL;
			f->sn->run(p);

			std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
			LOG(INFOL) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
		} catch(std::runtime_error e) {
			throwException(e.what());
			if (f->sn != NULL) {
				delete f->sn;
                f->sn = NULL;
			}
			return false;
		} catch(const char *e) {
            throwException ( e);
			if (f->sn != NULL) {
				delete f->sn;
                f->sn = NULL;
			}
			return false;
		} catch(std::bad_alloc e) {
			throwException( e.what());
			if (f->sn != NULL) {
				delete f->sn;
                f->sn = NULL;
			}
			return false;
		}
		f->sn->printCountAllIDBs("");
		return true;
	}

	/*
	 * Class:     karmaresearch_vlog_QueryResultIterator
	 * Method:    hasMoreElements
	 * Signature: (J)Z
	 */
	JNIEXPORT jboolean JNICALL Java_karmaresearch_vlog_QueryResultIterator_hasNext(JNIEnv *env, jobject obj, jlong ref) {
		TupleIterator *iter = (TupleIterator *) ref;
		if (iter == NULL) {
			return (jboolean) false;
		}
		return (jboolean) iter->hasNext();
	}

	/*
	 * Class:     karmaresearch_vlog_QueryResultIterator
	 * Method:    nextElement
	 * Signature: (J)[J
	 */
	JNIEXPORT jlongArray JNICALL Java_karmaresearch_vlog_QueryResultIterator_next(JNIEnv *env, jobject obj, jlong ref) {
		TupleIterator *iter = (TupleIterator *) ref;
		if (iter == NULL) {
			return NULL;
		}
		size_t sz = iter->getTupleSize();
		iter->next();
		jlong res[256];
		for (int i = 0; i < sz; i++) {
			res[i] = iter->getElementAt(i);
		}
		jlongArray outJNIArray = env->NewLongArray(sz);
		if (NULL == outJNIArray) return NULL;
		env->SetLongArrayRegion(outJNIArray, 0 , sz, res);
		return outJNIArray;
	}

	/*
	 * Class:     karmaresearch_vlog_QueryResultIterator
	 * Method:    cleanup
	 * Signature: (J)V
	 */
	JNIEXPORT void JNICALL Java_karmaresearch_vlog_QueryResultIterator_cleanup(JNIEnv *env, jobject obj, jlong ref) {
		TupleIterator *iter = (TupleIterator *) ref;
		if (iter != NULL) {
			delete iter;
		}
	}

	/*
	 * Class:     karmaresearch_vlog_QueryResultIterator
	 * Method:    hasBlanks
	 * Signature: ([J)Z
	 */
	JNIEXPORT jboolean JNICALL Java_karmaresearch_vlog_QueryResultIterator_hasBlanks(JNIEnv * env, jobject obj, jlongArray jv) {
		jsize sz = env->GetArrayLength(jv);
		jlong *e = env->GetLongArrayElements(jv, NULL);
		for (int i = 0; i < sz; i++) {
			if (IS_BLANK(e[i])) {
				env->ReleaseLongArrayElements(jv, e, JNI_ABORT);
				return (jboolean) true;
			}
		}
		env->ReleaseLongArrayElements(jv, e, JNI_ABORT);
		return (jboolean) false;
	}

#ifdef __cplusplus
}
#endif
