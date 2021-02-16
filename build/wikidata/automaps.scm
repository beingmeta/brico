;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata/automaps)

(when (file-exists? "local.cfg") (load-config "local.cfg"))
      
(use-module '{logger varconfig binio engine knodb})
(use-module '{brico brico/wikid brico/build/wikidata brico/build/wikidata/map})

(module-export! '{main})
(module-export! '{import-occupation})
(module-export! 
 '{import-actors
   import-lawyers
   import-musicians
   import-diplomats
   import-politicians
   import-occupations
   import-war-and-peace})

(define (make-brico-writable)
  (dbctl {brico.pool wikid.pool 
	  (dbctl brico.index 'partitions)
	  (dbctl wikid.index 'partitions)}
	 'readonly #f))

(define (get-specls wf)
  (let ((all wf)
	(next (?? wikidata.index @?wikid_genls wf)))
    (while (exists? (difference next all))
      (set+! all next)
      (set! next (??  wikidata.index @?wikid_genls next)))
    all))

(define (import-by-isa item wf bf (opts #f))
  (let* ((spec [@?genls* bf])
	 (known (?? 'wikidref (get item 'id)))
	 (candidates (try known (wikid/getmap item spec (opt+ opts 'lower #t)))))
    (cond ((exists? known))
	  ((singleton? candidates)
	   (logwarn |WikidMap| "Found new map for " item "\n   to " candidates))
	  ((and (fail? candidates) (getopt opts 'import #t))
	   (logwarn |WikidImport| 
	     "Imported " item "\n   into "
	     (wikid/import! item [lower #t] #f
			    [@?genls bf sensecat (get bf 'sensecat)])))
	  ((ambiguous? candidates)
	   (logwarn |Wikidmap|
	     "Ambiguous wikidata item " item
	     (do-choices (c candidates)
	       (printout "\n\t" c)))))))

(define (import-isa-type wf bf (opts #f))
  (do-choices (wf (if (getopt opts 'specls)
		      (get-specls wf)
		      wf))
    (when (and (getopt opts 'newcats)
	       (fail? (wikid/brico wf)))
      (wikid/import! wf))
    (if (config 'nthreads #t)
	(engine/run (lambda (item) (import-by-isa item wf bf opts))
	    (find-frames wikidata.index @?wikid_isa wf))
	(do-choices (item (find-frames wikidata.index @?wikid_isa wf))
	  (import-by-isa item wf bf opts)))))

(define (import-isa wf (opts #f))
  (do-choices (wf (if (getopt opts 'specls)
		      (get-specls wf)
		      wf))
    (when (and (getopt opts 'newcats) (fail? (wikid/brico wf)))
      (wikid/import! wf))
    (let ((bf (wikid/brico wf))
	  (items (find-frames wikidata.index @?wikid_isa wf)))
      (when (exists? items)
	(if (config 'nthreads #t)
	    (engine/run (lambda (item) (import-by-isa item wf bf opts))
		items)
	    (do-choices (item items)
	      (import-by-isa item wf bf opts))))))) 

(define (import-by-genls item wf bf (opts #f))
  (let* ((gspec [@?genls bf])
	 (g2spec [@?genls* bf])
	 (g3spec [@?genls* (get bf @?genls)])
	 (known (?? 'wikidref (get item 'id)))
	 (candidates (try known 
			  (wikid/getmap item gspec (opt+ opts 'lower #t))
			  (wikid/getmap item g2spec (opt+ opts 'lower #t))
			  (wikid/getmap item g3spec (opt+ opts 'lower #t)))))
    (cond ((exists? known))
	  ((singleton? candidates)
	   (logwarn |WikidMap| "Found new map for " item "\n   to " candidates))
	  ((and (fail? candidates) (getopt opts 'import #t))
	   (logwarn |WikidImport| 
	     "Imported " item "\n   into "
	     (wikid/import! item [lower #t] #f
			    [@?genls bf sensecat (get bf 'sensecat)])))
	  ((ambiguous? candidates)
	   (logwarn |Wikidmap| 
	     "Ambiguous wikidata item " item
	     (do-choices (c candidates)
	       (printout "\n\t" c)))))))

(define (import-dogs)
 (import-isa-type 
  @31c1/3c0f(wikidata "Q39367" norm "dog breed" 
		      gloss "group of closely related and visibly similar domestic dogs")
  @1/175a9(noun.animal "Canis familiaris" genls "canine" "domesticated animal")
  [lower #t]))

(define (import-cats)
 (import-isa-type 
  @31c1/488c(wikidata "Q43577" norm "cat breed" gloss "nogloss")
  @1/175b7(noun.animal "Felis domesticus" "domestic cat" "Felis catus" "house cat")
  [lower #t]))

(define (import-aircraft)
 (import-isa-type 
  @31c1/440e(wikidata "Q15056993" norm "aircraft family" gloss "group of related aircraft models sharing the same basic design")
  @1/17de9(noun.artifact "aircraft" genls "craft")
  [lower #t]))

(define (import-ships)
 (import-isa-type 
  @31c1/7a9e(wikidata "Q2235308" norm "ship type" gloss "group of ships of a similar purpose and function")
  @1/17cb4(noun.artifact "ship" genls "watercraft")
  [lower #t]))

(define (import-autos)
 (import-isa-type 
  @31c1/b820(wikidata "Q3231690" norm "automobile model" gloss "industrial automobile model associated with a brand, defined usually from an engineering point of view by a combination of chassis/bodywork")
  @1/f5dd(noun.artifact "automobile" genls "automotive vehicle")
  [lower #t]))

(define (import-productions)
  (wikidmap! @1/1b9b1(noun.communication "production")
	     @31c1/1a21a1(wikidata "Q43099500" norm "performing arts production" gloss "production of the performing arts, consisting of a run of quasi-identical performances of the same performance work"))
  (import-isa-type 
   @31c1/221cac6(wikidata "Q7777573" norm "theatrical style" gloss "nogloss")
   @1/1b9b1(noun.communication "production"))
  (import-isa-type 
   @31c1/24ea1(wikidata "Q15850590" norm "theatrical genre" gloss "division and subdivisions of various forms of theater")
   @1/1b9b1(noun.communication "production")))

(define (import-by-occupation item occupation isa (opts #f))
  (let* ((spec [@?isa isa])
	 (known (?? 'wikidref (get item 'id)))
	 (candidates (try known (wikid/getmap item spec [lower #f]))))
    (cond ((exists? known))
	  ((singleton? candidates)
	   (loginfo |WikidMap| "Found new map for " item "\n   to " candidates))
	  ((fail? candidates)
	   (lognotice |WikidImport| 
	     "Imported " item "\n   into " (wikid/import! item [lower #f])))
	  ((ambiguous? candidates)
	   (logwarn |Wikidmap| 
	     "Ambiguous wikidata item " item
	     (do-choices (c candidates)
	       (printout "\n\t" c)))))))

(define (import-occupation occupation (isa) (opts #f))
  (default! isa (?? 'wikidref (get occupation 'id)))
  (unless (testopt opts 'lower #f) (set! opts (opt+ opts 'lower #f)))
  (if (fail? occupation)
      (logwarn |NoCognate| "No mapped BRICO concept for " occupation)
      (let ((items (find-frames wikidata.index @?wikid_occupation occupation)))
	(when (exists? items)
	  (if (config 'nthreads #t)
	      (engine/run (def (import-occupation item) (import-by-occupation item occupation isa opts))
		  items)
	      (do-choices (item items)
		(import-by-occupation item occupation isa opts)))
	  (knodb/commit! {wikid.pool (pool/getindexes wikid.pool)
			  brico.pool (pool/getindexes brico.pool)})
	  (commit)
	  (swapout)))))

(define (add-stage-actor)
  (wikid/import! @31c1/1450(wikidata "Q2259451" norm "stage actor" gloss "")
		 [pool brico.pool]))
(define (add-tv-actor)
  (wikid/import! @31c1/1454(wikidata "Q10798782" norm "television actor")
		 [pool brico.pool]))

(define (import-lawyers)
  (import-occupation
   @31c1/1d(wikidata "Q40348" norm "lawyer" gloss "legal professional who helps clients and represents them in a court of law") 
   @1/1fef6(noun.person "attorney" genls "professional person")))

(define (import-actors)
  (import-occupation
   @31c1/fab(wikidata "Q33999" norm "actor" gloss "person who acts in a dramatic or comic production and works in film, television, theatre, or radio")
   @1/20103(noun.person "role player" genls "performing artist"))
  (import-occupation
   @31c1/1450(wikidata "Q2259451" norm "stage actor" gloss "")
   @1/af983(wikid "stage actor" "Q2259451"))
  (import-occupation
   @31c1/1454(wikidata "Q10798782" norm "television actor")
   @1/af984(wikid "television actor" "Q10798782"))
  (import-occupation
   @31c1/fa4(wikidata "Q10800557" norm "film actor" gloss "")
   @1/8c144(noun.person "screen actor" "movie actor")))

(define (import-politicians)
  (import-occupation
   @31c1/cd(wikidata "Q82955" norm "politician" gloss "person involved in politics, person who holds or seeks positions in government")
   @1/12e82(noun.person "politician" genls "leader")))

(define (import-diplomats)
  (import-occupation
   @31c1/ff3(wikidata "Q193391" norm "diplomat" gloss "person appointed by a state to conduct diplomacy with another state or international organization")
   @1/20706(noun.person "diplomatist" "diplomat")))

(define (import-musicians)
  (import-occupation
   @31c1/fad(wikidata "Q177220" norm "singer" gloss "person singing for a listening audience"))
  (import-occupation
   @31c1/f94(wikidata "Q488205" norm "singer-songwriter" gloss "musician who writes, composes and sings"))
  (import-occupation
   @31c1/fd3(wikidata "Q855091" norm "guitarist" gloss "person who plays the guitar"))
  (import-occupation
   @31c1/6c23(wikidata "Q386854" norm "drummer" gloss "percussionist who creates and accompanies music using drums"))
  (import-occupation
   @31c1/1a3c(wikidata "Q36834" norm "composer" gloss "person who creates music, either by musical notation or oral tradition"))
  (import-occupation 
   @31c1/145b(wikidata "Q3282637" norm "film producer" gloss "person who supervises the overall process, creative and financial, of making a film"))
  (import-occupation 
   @31c1/1452(wikidata "Q2405480" norm "voice actor" gloss "person who provides voice-overs for a character in films, animation, video games or in other media")))

(define religious-roots
  {@31c1/20f60(wikidata "Q3355750" norm "monastic order" gloss "uno de los tipos de orden religiosa católica") 	;;=#13.6
   @31c1/19436(wikidata "Q1530022" norm "religious organization" gloss "church or a society of some other religion") 	;;=#10.44
   @31c1/375cf(wikidata "Q2061186" norm "religious order" gloss "group of people set apart from society and other groups based on their religious devotion")
   @31c1/92da(wikidata "Q2742167" norm "religious community" gloss "community (group of people) who practice the same religion")
   ;; #10.23=
   @31c1/92e0(wikidata "Q13414953" norm "religious denomination" gloss "identifiable religious subgroup with a common structure and doctrine")
   @31c1/cc1085(wikidata "Q1068640" norm "folk religion" gloss "expressions of religion distinct from the official doctrines of organized religion")
   @31c1/1c04a(wikidata "Q28653" norm "mendicant order" gloss "Type of religious lifestyle") 	;;=#10.46
   @31c1/406dd(wikidata "Q995347" norm "Christian movement" gloss "theological, political, or philosophical interpretation of Christianity that is not generally represented by a specific church, sect, or denomination")
   @31c1/54de2(wikidata "Q2993243" norm "religious congregation" gloss "organization") 	;;=#10.68
   @31c1/5d053(wikidata "Q47280" norm "Abrahamic religion" gloss "category of religions considered as coming from the legacy of Abraham")
   @31c1/659a5(wikidata "Q222516" norm "school of Buddhism" gloss "institutional and doctrinal divisions of Buddhism that have existed from ancient times up to the present")
   @31c1/106d883(wikidata "Q23955632" norm "Catholic organization" gloss "organization sharing similar beliefs and goals as the Roman Catholic Church")
   @31c1/70285(wikidata "Q63188808" norm "Catholic religious occupation" gloss "nogloss") 	;;=#10.74
   @31c1/24c7b(wikidata "Q1826286" norm "religious movement" gloss "social and ideological movement in the religious sphere")
   @31c1/21b31(wikidata "Q879146" norm "Christian denomination" gloss "identifiable Christian body with common name, structure, and doctrine")
   })

(define university
  {@31c1/bd0d(wikidata "Q875538" norm "public university" gloss "university that is predominantly funded by public means") 	;;=#10.30)
   })

(define meta-roles
  {@31c1/142c(wikidata "Q28640" norm "profession" gloss "vocation founded upon specialized educational training") 	;;=#13.1
   @31c1/1e51(wikidata "Q12737077" norm "occupation" gloss "label applied to a person based on an activity they participate in")})

(define (import-occupations)
  (let* ((occupations (pick (wikidata/find @?wikid_isa meta-roles)
			wikidata->brico))
	 (done (try (file-exists? "occupations.xtype") (read-xtype (open-byte-input "occupations.xtype")))))
    (do-choices (occupation (difference occupations done))
      (logwarn |Occupation| "Importing " occupation " " (get occupation '%id))
      (import-occupation occupation)
      (write-xtype occupation (extend-byte-output "occupations.xtype"))
      (logwarn |Occupation| "Finished importing " occupation))))

(define wikidata-war 
  @31c1/2b1e(wikidata "Q198" norm "war" gloss "organised and prolonged violent conflict between states"))
(define brico-war @1/1539f(noun.act "war" genls "crusade"))
(define war-types
  (filter-choices (f (find-frames wikidata.index @?wikid_genls wikidata-war))
    (exists? (find-frames wikidata.index @?wikid_isa f))))

(define wikidata-battle
  @31c1/461d(wikidata "Q178561" norm "battle" gloss "part of a war which is well defined in duration, area and force commitment"))
(define brico-battle (?? 'wikidref "Q178561"))
(define battle-types
  (filter-choices (f (find-frames wikidata.index @?wikid_genls wikidata-battle))
    (exists? (find-frames wikidata.index @?wikid_isa f))))

(define wikidata-treaty-maps
  [@31c1/20759(wikidata "Q321839" norm "accord" gloss "agreement between two or more contracting persons or parties")
   @1/22fc1(noun.state "agreement" "accord")
   @31c1/15464(wikidata "Q131569" norm "treaty" gloss "express agreement under international law entered into by actors in international law")
   @1/1c7dc(noun.communication "treaty" genls "written agreement")])

(define (import-war-and-peace)
  (when (fail? brico-battle)
    (wikidmap! @1/155fe(noun.act "engagement" "conflict" "battle" "fight")
	       wikidata-battle)
    (set! brico-battle @1/155fe(noun.act "engagement" "conflict" "battle" "fight")))
  (import-by-genls war-types wikidata-war brico-war)
  (import-by-genls battle-types wikidata-battle brico-battle)
  (do-choices (type {war-types battle-types})
    (import-isa-type type (wikid/brico type) [lower #f]))
  (do-choices (wf (getkeys wikidata-treaty-maps))
    (wikidmap! (get wikidata-treaty-maps wf) wf))
  (do-choices (wf (getkeys wikidata-treaty-maps))
    (import-isa-type wf (get wikidata-treaty-maps wf) [lower #f])))

(define (main (importer (config 'importer)))
  (config! 'brico:readonly #f)
  (config! 'wikid:readonly #f)
  (when importer
    (unless (symbol? importer) (set! importer (getsym importer)))
    (when (and importer (symbol-bound? importer))
      ((eval importer))))
  (unless importer
    (import-actors)
    (import-lawyers)
    (import-musicians)
    (import-diplomats)
    (import-politicians)
    (import-occupations)
    (import-war-and-peace))
  (knodb/commit))

(when (config 'optimized #f)
  (optimize! '{brico brico/wikid brico/indexing
	       brico/build/wikidata brico/build/wikidata/map
	       engine})
  (optimize!))

#|
(wikidmap! 
  @1/8b176(noun.person "Richard Starkey" genls "dessert apple")
  @31c1/2805e(wikidata "Q2632" norm "Ringo Starr" gloss "British musician, drummer for the Beatles"))
(wikidmap! 
  @1/800593a(wikid "Ringo Starr" "Q2632")
  @31c1/3def99a(wikidata "Q41781790" norm "Starr" gloss "apple cultivar"))
|#
