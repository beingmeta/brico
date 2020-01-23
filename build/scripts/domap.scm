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

(define dog-breed
  @31c1/3c0f(wikidata "Q39367" norm "dog breed" gloss "group of closely related and visibly similar domestic dogs"))
(define dog-breeds 
  (find-frames wikidata.index  @?wikid_isa dog-breed))
(define brico-dog
  @1/175a9(noun.animal "Canis familiaris" genls "canine" "domesticated animal"))

(define (import-dogs)
  (let* ((spec [@?genls* brico-dog])
	 (brico (wikid/getmap dog-breeds spec [downcase #t]))
	 (handled (wikid/ref (get brico 'wikidref))))
    (for-choices (copy (difference dog-breeds handled))
      (wikid/import! copy [lower #f] #f [@?genls brico-dog sensecat 'noun.animal]))))

(define cat-breed
  @31c1/488c(wikidata "Q43577" norm "cat breed" gloss "nogloss"))
(define cat-breeds 
  (find-frames wikidata.index  @?wikid_isa cat-breed))
(define domestic-cat
  @1/175b7(noun.animal "Felis domesticus" "domestic cat" "Felis catus" "house cat"))

(define (import-cats)
  (let* ((spec [@?genls* domestic-cat])
	 (brico (wikid/getmap cat-breeds spec [downcase #t]))
	 (handled (wikid/ref (get brico 'wikidref))))
    (for-choices (copy (difference cat-breeds handled))
      (wikid/import! copy [lower #f] #f [@?genls domestic-cat sensecat 'noun.animal]))))

(define horse-breed
  @31c1/170d1(wikidata "Q1160573" norm "horse breed" gloss "selectively bred form of the domesticated horse"))
(define horse-breeds
  (find-frames wikidata.index @?wikid_isa horse-breed))
(define horse
  @1/e3bb(noun.animal "Equus caballus" "horse"))

(define (import-horses)
  (let* ((spec [@?genls* horse])
	 (brico (wikid/getmap horse-breeds spec [downcase #t]))
	 (handled (wikid/ref (get brico 'wikidref))))
    (for-choices (copy (difference horse-breeds handled))
      (wikid/import! copy [lower #f] #f [@?genls horse sensecat 'noun.animal]))))

(define auto-model
  @31c1/b820(wikidata "Q3231690" norm "automobile model" gloss "industrial automobile model associated with a brand, defined usually from an engineering point of view by a combination of chassis/bodywork"))
(define auto-models
  (find-frames wikidata.index @?wikid_isa auto-model))
(define auto
  @1/f5dd(noun.artifact "automobile" genls "automotive vehicle"))

(define (import-autos)
  (let* ((spec [@?genls* auto])
	 (brico (wikid/getmap auto-models spec [downcase #t]))
	 (handled (wikid/ref (get brico 'wikidref))))
    (logwarn |Wikidmap| "Found existing matches for " (|| handled) " entries")
    (for-choices (copy (difference auto-models handled))
      (logwarn |WikiImport|
	(wikid/import! copy [lower #f] #f [@?genls auto sensecat 'noun.artifact])))))
