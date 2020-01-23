(load-config (get-component "local.cfg"))
(use-module '{brico/build/wikidata brico/build/wikidmap})
(dbctl {brico.pool wikid.pool 
	(dbctl brico.index 'partitions)
	(dbctl wikid.index 'partitions)}
       'readonly #f)

(when (config 'optimized #t)
  (optimize! '{brico brico/wikid brico/indexing
	       brico/build/wikidata brico/build/wikidmap
	       engine}))

(define (import-isa-item item wf bf (opts #f))
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
  (do-choices (item (find-frames wikidata.index @?wikid_isa wf))
    (import-isa-item item wf bf opts)))

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

(define (import-horses)
 (import-isa-type 
  @31c1/170d1(wikidata "Q1160573" norm "horse breed" gloss "selectively bred form of the domesticated horse")
  @1/e3bb(noun.animal "Equus caballus" "horse")
  [lower #t]))

(define (import-autos)
 (import-isa-type 
  @31c1/b820(wikidata "Q3231690" norm "automobile model" gloss "industrial automobile model associated with a brand, defined usually from an engineering point of view by a combination of chassis/bodywork")
  @1/f5dd(noun.artifact "automobile" genls "automotive vehicle")
  [lower #t]))

(define (import-occupation occupation isa (opts #f))
  (do-choices (item (find-frames wikidata.index @?wikid_occupation occupation))
    (let* ((spec [@?isa isa])
	   (known (?? 'wikidref (get item 'id)))
	   (candidates (try known (wikid/getmap item spec [lower #f]))))
      (cond ((exists? known))
	    ((singleton? candidates)
	     (logwarn |WikidMap| "Found new map for " item "\n   to " candidates))
	    ((fail? candidates)
	     (logwarn |WikidImport| 
	       "Imported " item "\n   into " (wikid/import! item [lower #f])))
	    ((ambiguous? candidates)
	     (logwarn |Wikidmap| 
	       "Ambiguous wikidata item " item
	       (do-choices (c candidates)
		 (printout "\n\t" c))))))))

(define (import-attorneys)
  (import-occupation
   @31c1/1d(wikidata "Q40348" norm "lawyer" gloss "legal professional who helps clients and represents them in a court of law") 
   @1/1fef6(noun.person "attorney" genls "professional person")
   [lower #f]))

(define (import-politicians)
  (import-occupation
   @31c1/cd(wikidata "Q82955" norm "politician" gloss "person involved in politics, person who holds or seeks positions in government")
   @1/12e82(noun.person "politician" genls "leader")
   [lower #f]))

(define (import-diplomats)
  (import-occupation
   @31c1/ff3(wikidata "Q193391" norm "diplomat" gloss "person appointed by a state to conduct diplomacy with another state or international organization")
   @1/20706(noun.person "diplomatist" "diplomat")
   [lower #f]))

(optimize!)

