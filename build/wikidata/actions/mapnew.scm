;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'brico/build/wikidata/actions/mapnew)

(when (file-exists? "local.cfg") (load-config "local.cfg"))
      
(use-module '{logger varconfig binio engine optimize text/stringfmts knodb})
(use-module '{brico brico/wikid brico/build/wikidata
	      brico/build/wikidata/map brico/build/wikidata/automap})

(define (getdone aspect) (mkpath wikidata.dir (glom aspect ".xtype")))

(module-export! '{main})
(module-export! 
 '{import-actors
   import-lawyers
   import-musicians
   import-diplomats
   import-politicians
   import-occupations
   import-war-and-peace
   import-art})
(module-export! '{import-art-opts})

(module-export!
 '{import-art
   import-dogs
   import-cats
   import-aircraft
   import-ships
   import-autos
   import-productions})

(define %optmods
  '{brico brico/wikid brico/indexing
    brico/build/wikidata brico/build/wikidata/map
    brico/build/wikidata/automap brico/build/wikidata/actions/mapnew
    engine})

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


(define (import-dogs)
 (import-isa-type 
  (wikidata/ref "Q39367" "dog breed")
  @1/175a9(noun.animal "Canis familiaris" genls "canine" "domesticated animal")
  [lower #t]))

(define (import-cats)
 (import-isa-type 
  (wikidata/ref "Q43577" "cat breed")
  @1/175b7(noun.animal "Felis domesticus" "domestic cat" "Felis catus" "house cat")
  [lower #t]))

(define (import-aircraft)
 (import-isa-type 
  (wikidata/ref "Q15056993" "aircraft family")
  @1/17de9(noun.artifact "aircraft" genls "craft")
  [lower #t]))

(define (import-ships)
 (import-isa-type 
  (wikidata/ref "Q2235308" "ship type")
  @1/17cb4(noun.artifact "ship" genls "watercraft")
  [lower #t]))

(define (import-autos)
 (import-isa-type 
  (wikidata/ref "Q3231690" "automobile model")
  @1/f5dd(noun.artifact "automobile" genls "automotive vehicle")
  [lower #t]))

(define (import-productions)
  (wikidmap! @1/1b9b1(noun.communication "production") 
	     (wikidata/ref "Q43099500" "performing args production"))
  (import-isa-type 
   (wikidata/ref "Q7777573" "theatrical genre")
   @1/1b9b1(noun.communication "production")))

(define (add-stage-actor)
  (wikid/import! (wikidata/ref "Q2259451" "stage actor") [pool brico.pool]))
(define (add-tv-actor)
  (wikid/import! (wikidata/ref "Q10798782" "television actor") [pool brico.pool]))

(define (import-lawyers)
  (import-occupation
   (wikidata/ref "Q40348" "lawyer") 
   @1/1fef6(noun.person "attorney" genls "professional person")))

(define (import-actors)
  (import-occupation
   (wikidata/ref "Q33999" "actor")
   @1/20103(noun.person "role player" genls "performing artist"))
  (import-occupation
   (wikidata/ref "Q2259451" "stage actor")
   @1/af983(wikid "stage actor" "Q2259451"))
  (import-occupation
   (wikidata/ref "Q10798782" "television actor")
   @1/af984(wikid "television actor" "Q10798782"))
  (import-occupation
   (wikidata/ref "Q10800557" "film actor")
   @1/8c144(noun.person "screen actor" "movie actor")))

(define (import-politicians)
  (import-occupation
   (wikidata/ref "Q82955" "politician")
   @1/12e82(noun.person "politician" genls "leader")))

(define (import-diplomats)
  (import-occupation
   (wikidata/ref "Q193391" "diplomat")
   @1/20706(noun.person "diplomatist" "diplomat")))

(define (import-musicians)
  (import-occupation
   (wikidata/ref "Q177220" "singer"))
  (import-occupation
   (wikidata/ref "Q488205" "singer-songwriter"))
  (import-occupation
   (wikidata/ref "Q855091" "guitarist"))
  (import-occupation
   (wikidata/ref "Q386854" "drummer"))
  (import-occupation
   (wikidata/ref "Q36834" "composer"))
  (import-occupation 
   (wikidata/ref "Q3282637" "film producer"))
  (import-occupation 
   (wikidata/ref "Q2405480" "voice actor")))

(define religious-roots
  {(wikidata/ref "Q3355750" "monastic order") 	;;=#13.6
   (wikidata/ref "Q1530022" "religious organization") 	;;=#10.44
   (wikidata/ref "Q2061186" "religious order")
   (wikidata/ref "Q2742167" "religious community")
   ;; #10.23=
   (wikidata/ref "Q13414953" "religious denomination")
   (wikidata/ref "Q1068640" "folk religion")
   (wikidata/ref "Q28653" "mendicant order") 	;;=#10.46
   (wikidata/ref "Q995347" "Christian movement")
   (wikidata/ref "Q2993243" "religious congregation") 	;;=#10.68
   (wikidata/ref "Q47280" "Abrahamic religion")
   (wikidata/ref "Q222516" "school of Buddhism")
   (wikidata/ref "Q23955632" "Catholic organization")
   (wikidata/ref "Q63188808" "Catholic religious occupation") 	;;=#10.74
   (wikidata/ref "Q1826286" "religious movement")
   (wikidata/ref "Q879146" "Christian denomination")
   })

(define university
  {(wikidata/ref "Q875538" "public university") 	;;=#10.30)
   })

(define meta-roles
  {(wikidata/ref "Q28640" "profession") 	;;=#13.1
   (wikidata/ref "Q12737077" "occupation")})

(define (get-occupations)
  (pick (wikidata/find @?wikid_isa meta-roles) wikidata->brico))
(module-export! 'get-occupations)

(define (import-occupations)
  (let* ((occupations (pick (wikidata/find @?wikid_isa meta-roles)
			wikidata->brico))
	 (done (try (file-exists? (getdone "occupations"))
		    (read-xtype (open-byte-input (getdone "occupations"))))))
    (do-choices (occupation (difference occupations done))
      (logwarn |Occupation| "Importing " occupation " " (get occupation '%id))
      (import-occupation occupation)
      (write-xtype occupation (getdone "occupations"))
      (logwarn |Occupation| "Finished importing " occupation))))

(define wikidata-war 
  (wikidata/ref "Q198" "war"))
(define brico-war @1/1539f(noun.act "war" genls "crusade"))
(define war-types
  (filter-choices (f (find-frames wikidata.index @?wikid_genls wikidata-war))
    (exists? (find-frames wikidata.index @?wikid_isa f))))

(define wikidata-battle
  (wikidata/ref "Q178561" "battle"))
(define brico-battle (?? 'wikidref "Q178561"))
(define battle-types
  (filter-choices (f (find-frames wikidata.index @?wikid_genls wikidata-battle))
    (exists? (find-frames wikidata.index @?wikid_isa f))))

(define wikidata-treaty-maps
  `#[,(wikidata/ref "Q321839" "accord")
     @1/22fc1(noun.state "agreement" "accord")
     ,(wikidata/ref "Q131569" "treaty")
     @1/1c7dc(noun.communication "treaty" genls "written agreement")])

(define (import-war-and-peace)
  (when (fail? brico-battle)
    (wikidmap! @1/155fe(noun.act "engagement" "conflict" "battle" "fight")
	       wikidata-battle)
    (set! brico-battle @1/155fe(noun.act "engagement" "conflict" "battle" "fight")))
  (import-by-genls war-types wikidata-war brico-war)
  (import-by-genls battle-types wikidata-battle brico-battle)
  (do-choices (type {war-types battle-types})
    (logwarn |Importing| type)
    (import-isa-type type (wikid/brico type) [lower #f]))
  (do-choices (wf (getkeys wikidata-treaty-maps))
    (wikidmap! (get wikidata-treaty-maps wf) wf))
  (do-choices (wf (getkeys wikidata-treaty-maps))
    (import-isa-type wf (get wikidata-treaty-maps wf) [lower #f])))

(define import-art-opts [specls #t newcats #f])

(define (import-art)
  (unless (exists? (?? 'wikidref "Q2743"))
    (wikidmap! @1/18a35(noun.communication "musical theater" "musical comedy" "musical")
	       @31c1/ab7(wikidata "Q2743" norm "musical theatre")))
  (import-isa @31c1/2ca0(wikidata "Q11424" norm "film") import-art-opts)
  (import-isa @31c1/2bdf4(wikidata "Q179700" norm "statue") import-art-opts)
  (import-isa @31c1/326efd(wikidata "Q3305213" norm "painting") import-art-opts)
  (import-isa @31c1/64a77b9(wikidata "Q105543609" norm "musical work/composition") import-art-opts)
  (import-isa @31c1/6323(wikidata "Q25379" norm "play") import-art-opts)
  (import-isa @31c1/4f1eff(wikidata "Q5185279" norm "poem") import-art-opts)
  (import-isa @31c1/21639d(wikidata "Q2188189" norm "musical work") import-art-opts)
  (import-isa @31c1/ab7(wikidata "Q2743" norm "musical theatre") import-art-opts)
  (import-isa @31c1/540(wikidata "Q1344" norm "opera") import-art-opts)
  (import-isa @31c1/525f9a(wikidata "Q5398426" norm "television series") import-art-opts)
  (import-isa @31c1/75eb2(wikidata "Q482994" norm "album") import-art-opts)
  ;; (import-isa @31c1/291a56c(wikidata "Q43099500" norm "performing arts production"))
  )

(define (main (importer (config 'importer)))
  (config! 'brico:readonly #f)
  (config! 'wikid:readonly #f)
  (config-default! 'wikidata:skipindex #t)
  (unless (config 'smoketest)
    (when importer
      (unless (symbol? importer) (set! importer (getsym importer)))
      (when (and importer (symbol-bound? importer))
	((eval importer))))
    (unless importer
      (import-occupations)
      (import-war-and-peace)
      ;; f(import-art) (import-dogs) (import-cats) (import-aircraft) (import-ships) (import-autos)(import-productions)
      )
    (knodb/commit)))
