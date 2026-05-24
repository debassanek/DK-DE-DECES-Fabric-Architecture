# DK-BI — Mesures DAX · Pages Détail
> Projet : Analyse de la Mortalité en France (data.gouv.fr)  
> Modèle : fact_deces + dim_date + dim_age + dim_lieu + dim_generation  
> Toutes les mesures sont à créer dans une table dédiée `[_Mesures]`

---

## 0. Mesures de base (socle partagé)

```dax
-- Nombre total de décès (mesure de base)
[Nb Décès] =
COUNTROWS( fact_deces )

-- Nombre de décès femmes
[Nb Décès Femmes] =
CALCULATE( [Nb Décès], fact_deces[sexe] = "F" )

-- Nombre de décès hommes
[Nb Décès Hommes] =
CALCULATE( [Nb Décès], fact_deces[sexe] = "M" )

-- Part femmes
[% Femmes] =
DIVIDE( [Nb Décès Femmes], [Nb Décès] )

-- Âge moyen au décès
[Âge Moyen Décès] =
AVERAGE( fact_deces[age_au_deces] )

-- Âge médian au décès
[Âge Médian Décès] =
MEDIAN( fact_deces[age_au_deces] )
```

---

## 1. PAGE — Analyse Centenaires (100+)

```dax
-- Total décès centenaires (100 ans et plus)
[Nb Centenaires] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] >= 100
)

-- Total super-centenaires (110+)
[Nb Super-centenaires] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] >= 110
)

-- Part centenaires dans le total
[% Centenaires] =
DIVIDE( [Nb Centenaires], [Nb Décès] )

-- Âge moyen des centenaires au décès
[Âge Moyen Centenaires] =
CALCULATE(
    AVERAGE( fact_deces[age_au_deces] ),
    fact_deces[age_au_deces] >= 100
)

-- Part femmes parmi les centenaires
[% Femmes Centenaires] =
DIVIDE(
    CALCULATE( [Nb Centenaires], fact_deces[sexe] = "F" ),
    [Nb Centenaires]
)

-- Évolution YoY centenaires
[Évolution YoY Centenaires] =
VAR _AnneeActuelle = CALCULATE( [Nb Centenaires], LASTDATE( dim_date[date] ) )
VAR _AnneePrec     = CALCULATE(
    [Nb Centenaires],
    DATEADD( dim_date[date], -1, YEAR )
)
RETURN DIVIDE( _AnneeActuelle - _AnneePrec, _AnneePrec )

-- Tranche 100–104 ans
[Centenaires 100-104] =
CALCULATE( [Nb Décès], fact_deces[age_au_deces] >= 100, fact_deces[age_au_deces] <= 104 )

-- Tranche 105–109 ans
[Centenaires 105-109] =
CALCULATE( [Nb Décès], fact_deces[age_au_deces] >= 105, fact_deces[age_au_deces] <= 109 )

-- Part tranche 100-104 parmi centenaires
[% Centenaires 100-104] =
DIVIDE( [Centenaires 100-104], [Nb Centenaires] )
```

---

## 2. PAGE — Mortalité des Mineurs (<18 ans)

```dax
-- Total décès mineurs (< 18 ans)
[Nb Décès Mineurs] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] < 18
)

-- Mortalité nourrissons (0–1 an)
[Nb Décès 0-1 an] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] <= 1
)

-- Mortalité 1–4 ans
[Nb Décès 1-4 ans] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] >= 1,
    fact_deces[age_au_deces] <= 4
)

-- Mortalité 5–14 ans
[Nb Décès 5-14 ans] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] >= 5,
    fact_deces[age_au_deces] <= 14
)

-- Mortalité 15–17 ans
[Nb Décès 15-17 ans] =
CALCULATE(
    [Nb Décès],
    fact_deces[age_au_deces] >= 15,
    fact_deces[age_au_deces] < 18
)

-- Part nourrissons parmi mineurs
[% Nourrissons / Mineurs] =
DIVIDE( [Nb Décès 0-1 an], [Nb Décès Mineurs] )

-- Part garçons parmi mineurs
[% Garçons Mineurs] =
DIVIDE(
    CALCULATE( [Nb Décès Mineurs], fact_deces[sexe] = "M" ),
    [Nb Décès Mineurs]
)

-- Tendance décès mineurs (régression linéaire simplifiée, pente YoY)
[Tendance Mineurs YoY] =
VAR _Current = CALCULATE( [Nb Décès Mineurs], YEAR( MAX(dim_date[date]) ) = YEAR( MAX(dim_date[date]) ) )
VAR _Prev    = CALCULATE( [Nb Décès Mineurs], DATEADD( dim_date[date], -1, YEAR ) )
RETURN DIVIDE( _Current - _Prev, _Prev )
```

---

## 3. PAGE — Profil Commune

```dax
-- Décès dans la commune sélectionnée (via slicer dim_lieu[code_insee])
[Nb Décès Commune] =
CALCULATE(
    [Nb Décès],
    SELECTEDVALUE( dim_lieu[code_insee_commune] )
)

-- Rang de la commune par volume de décès (national)
[Rang Commune National] =
RANKX(
    ALL( dim_lieu[code_insee_commune] ),
    CALCULATE( [Nb Décès] ),
    ,
    DESC,
    DENSE
)

-- Âge moyen décès commune vs national
[Âge Moyen Commune] =
CALCULATE(
    AVERAGE( fact_deces[age_au_deces] ),
    SELECTEDVALUE( dim_lieu[code_insee_commune] )
)

[Écart Âge vs National] =
[Âge Moyen Commune] - CALCULATE( [Âge Moyen Décès], ALL( dim_lieu ) )

-- % de 85 ans et plus dans la commune
[% 85+ Commune] =
DIVIDE(
    CALCULATE( [Nb Décès], fact_deces[age_au_deces] >= 85 ),
    [Nb Décès]
)

-- Écart % 85+ commune vs national
[Écart % 85+ vs National] =
[% 85+ Commune]
- CALCULATE( DIVIDE( CALCULATE([Nb Décès], fact_deces[age_au_deces] >= 85), [Nb Décès] ), ALL(dim_lieu) )

-- Décès moyen par mois (saisonnalité)
[Décès Moy par Mois] =
AVERAGEX(
    VALUES( dim_date[mois_numero] ),
    CALCULATE( [Nb Décès] )
)

-- Indice de saisonnalité du mois courant
[Indice Saisonnalité] =
DIVIDE( [Nb Décès], [Décès Moy par Mois] )
```

---

## 4. PAGE — Anomalies Géographiques

```dax
-- Décès attendus (moyenne nationale par habitant * population estimée)
-- Nécessite une table de référence population par commune
[Décès Attendus Commune] =
VAR _TauxNational = CALCULATE(
    DIVIDE( [Nb Décès], SUM( dim_lieu[population_estimee] ) ),
    ALL( dim_lieu )
)
RETURN _TauxNational * MAX( dim_lieu[population_estimee] )

-- Z-Score de mortalité communale
[Z-Score Commune] =
VAR _Obs   = [Nb Décès]
VAR _Moy   = CALCULATE( AVERAGEX( ALL(dim_lieu[code_insee_commune]), [Nb Décès] ) )
VAR _Ecart = CALCULATE(
    STDEVX.P( ALL(dim_lieu[code_insee_commune]), [Nb Décès] )
)
RETURN DIVIDE( _Obs - _Moy, _Ecart )

-- Niveau d'anomalie (libellé)
[Niveau Anomalie] =
VAR _z = [Z-Score Commune]
RETURN
    SWITCH(
        TRUE(),
        ABS(_z) > 3,   "CRITIQUE",
        ABS(_z) > 2.5, "ÉLEVÉ",
        ABS(_z) > 2,   "MODÉRÉ",
        ABS(_z) > 1.5, "LÉGER",
        "NORMAL"
    )

-- Sens de l'anomalie
[Sens Anomalie] =
VAR _z = [Z-Score Commune]
RETURN IF( _z > 0, "Surmortalité", "Sous-mortalité" )

-- Nombre de communes anomalies (z > seuil)
[Nb Communes Anomalies] =
CALCULATE(
    DISTINCTCOUNT( fact_deces[id_commune] ),
    FILTER(
        ALL( dim_lieu ),
        ABS( [Z-Score Commune] ) > 2.5
    )
)
```

---

## 5. PAGE — Zoom Année

```dax
-- Décès pour l'année sélectionnée
[Nb Décès Année] =
CALCULATE(
    [Nb Décès],
    YEAR( dim_date[date] ) = SELECTEDVALUE( dim_date[annee] )
)

-- Décès année précédente (pour comparaison)
[Nb Décès Année-1] =
CALCULATE(
    [Nb Décès],
    DATEADD( dim_date[date], -1, YEAR )
)

-- Variation YoY
[Variation YoY %] =
DIVIDE( [Nb Décès] - [Nb Décès Année-1], [Nb Décès Année-1] )

-- Décès par mois avec contexte de filtre année
[Nb Décès Mensuel] =
CALCULATE(
    [Nb Décès],
    ALLEXCEPT( dim_date, dim_date[mois_numero], dim_date[annee] )
)

-- Mois avec le plus de décès (dans l'année sélectionnée)
[Mois Max Décès] =
CALCULATE(
    MAXX(
        VALUES( dim_date[mois_libelle] ),
        [Nb Décès]
    )
)

-- Surmortalité hivernale (Nov–Fév vs Juin–Sep)
[Surmortalité Hivernale] =
VAR _Hiver = CALCULATE( [Nb Décès], dim_date[mois_numero] IN {11,12,1,2} )
VAR _Ete   = CALCULATE( [Nb Décès], dim_date[mois_numero] IN {6,7,8,9} )
RETURN DIVIDE( _Hiver - _Ete, _Ete )

-- Âge moyen mensuel (pour sparkline)
[Âge Moyen Mensuel] =
AVERAGEX(
    FILTER( fact_deces, RELATED(dim_date[annee]) = SELECTEDVALUE(dim_date[annee]) ),
    fact_deces[age_au_deces]
)
```

---

## 6. PAGE — Événements Exceptionnels

```dax
-- Baseline : moyenne des décès sur les 5 années précédentes (même mois)
[Baseline 5 ans] =
CALCULATE(
    AVERAGEX(
        DATESINPERIOD( dim_date[date], FIRSTDATE(dim_date[date]), -5, YEAR ),
        [Nb Décès]
    )
)

-- Surmortalité observée vs baseline
[Surmortalité vs Baseline] =
DIVIDE( [Nb Décès] - [Baseline 5 ans], [Baseline 5 ans] )

-- Surmortalité COVID 2020 (Avril uniquement)
[Surmortalité COVID Avr-2020] =
VAR _2020 = CALCULATE(
    [Nb Décès],
    YEAR(dim_date[date]) = 2020,
    MONTH(dim_date[date]) = 4
)
VAR _Baseline = CALCULATE(
    AVERAGEX(
        FILTER( ALL(dim_date), dim_date[annee] >= 2015 && dim_date[annee] <= 2019 && dim_date[mois_numero] = 4 ),
        [Nb Décès]
    )
)
RETURN DIVIDE( _2020 - _Baseline, _Baseline )

-- Étiquette événement (pour annotations sur graphique)
[Label Événement] =
VAR _annee = SELECTEDVALUE( dim_date[annee] )
VAR _mois  = SELECTEDVALUE( dim_date[mois_numero] )
RETURN
    SWITCH(
        TRUE(),
        _annee = 2020 && _mois IN {3,4,5},   "COVID V1",
        _annee = 2020 && _mois IN {10,11,12}, "COVID V2",
        _annee = 2021 && _mois IN {1,2,3},    "COVID V3",
        _annee = 2019 && _mois IN {7,8},      "Canicule 2019",
        _annee = 2022 && _mois IN {6,7,8},    "Canicule 2022",
        _annee = 2017 && _mois IN {1,2},      "Grippe A 2017",
        BLANK()
    )

-- Indicateur binaire "événement exceptionnel"
[Est Événement] =
IF( ABS([Surmortalité vs Baseline]) > 0.05, 1, 0 )
```

---

## 7. PAGE — Profil Génération

```dax
-- Filtrage par génération via dim_generation
[Nb Décès Génération] =
CALCULATE(
    [Nb Décès],
    SELECTEDVALUE( dim_generation[generation_libelle] )
)

-- Âge moyen au décès pour la génération sélectionnée
[Âge Moyen Génération] =
CALCULATE(
    AVERAGE( fact_deces[age_au_deces] ),
    SELECTEDVALUE( dim_generation[generation_libelle] )
)

-- Écart vs génération précédente
[Écart Âge vs Gén. Préc.] =
VAR _GenActuelle = SELECTEDVALUE( dim_generation[ordre_chronologique] )
VAR _AgeActuelle = [Âge Moyen Génération]
VAR _AgePrec     = CALCULATE(
    AVERAGE( fact_deces[age_au_deces] ),
    FILTER(
        ALL( dim_generation ),
        dim_generation[ordre_chronologique] = _GenActuelle - 1
    )
)
RETURN _AgeActuelle - _AgePrec

-- Courbe de survie (% encore vivant à chaque âge) — calcul cumulatif
[% Survie à l'Âge X] =
VAR _AgeCourant = SELECTEDVALUE( dim_age[age] )
VAR _TotalGen   = CALCULATE( [Nb Décès Génération], ALL(dim_age) )
VAR _DecesAvant = CALCULATE(
    [Nb Décès Génération],
    FILTER( ALL(dim_age), dim_age[age] <= _AgeCourant )
)
RETURN 1 - DIVIDE( _DecesAvant, _TotalGen )

-- Espérance de vie résiduelle estimée (simplifiée)
[Espérance de Vie Résiduelle] =
VAR _AgeMoyen = [Âge Moyen Génération]
VAR _AgeMoyenNational = CALCULATE( [Âge Moyen Décès], ALL( dim_generation ) )
RETURN _AgeMoyenNational - _AgeMoyen

-- Comparaison inter-générations (mesure de table)
[Âge Moyen Par Génération] =
AVERAGEX(
    VALUES( dim_generation[generation_libelle] ),
    CALCULATE( AVERAGE( fact_deces[age_au_deces] ) )
)
```

---

## 8. PAGE — Anomalies Générationnelles

```dax
-- Âge moyen attendu pour une cohorte (interpolation linéaire des cohortes adjacentes)
[Âge Moyen Attendu Cohorte] =
VAR _AnNaissance  = SELECTEDVALUE( fact_deces[annee_naissance] )
VAR _MoyGlobale   = CALCULATE( AVERAGE(fact_deces[age_au_deces]), ALL(fact_deces) )
VAR _Ecart        = CALCULATE(
    STDEVX.P( ALL(fact_deces[annee_naissance]),
              CALCULATE(AVERAGE(fact_deces[age_au_deces])) )
)
-- Approche simplifiée : régression sur cohortes N-2 à N+2
VAR _Voisines = FILTER(
    ALL( fact_deces[annee_naissance] ),
    fact_deces[annee_naissance] >= _AnNaissance - 3
    && fact_deces[annee_naissance] <= _AnNaissance + 3
    && fact_deces[annee_naissance] <> _AnNaissance
)
RETURN CALCULATE( AVERAGE(fact_deces[age_au_deces]), _Voisines )

-- Z-Score générationnel
[Z-Score Génération] =
VAR _Obs   = CALCULATE( AVERAGE(fact_deces[age_au_deces]) )
VAR _Moy   = CALCULATE( AVERAGEX( ALL(fact_deces[annee_naissance]), CALCULATE(AVERAGE(fact_deces[age_au_deces])) ) )
VAR _Sigma = CALCULATE( STDEVX.P( ALL(fact_deces[annee_naissance]), CALCULATE(AVERAGE(fact_deces[age_au_deces])) ) )
RETURN DIVIDE( _Obs - _Moy, _Sigma )

-- Niveau d'anomalie générationnelle
[Niveau Anomalie Génération] =
SWITCH(
    TRUE(),
    ABS([Z-Score Génération]) > 3,   "CRITIQUE",
    ABS([Z-Score Génération]) > 2.5, "ÉLEVÉ",
    ABS([Z-Score Génération]) > 2,   "MODÉRÉ",
    ABS([Z-Score Génération]) > 1.5, "LÉGER",
    "NORMAL"
)

-- Impact COVID par cohorte (surmortalité 2020 vs baseline 2015-2019)
[Surmortalité COVID Cohorte] =
VAR _2020    = CALCULATE( [Nb Décès], YEAR(dim_date[date]) = 2020 )
VAR _Baseline = CALCULATE(
    AVERAGEX( {2015,2016,2017,2018,2019},
        CALCULATE([Nb Décès], YEAR(dim_date[date]) = [Value]) ),
    ALL(dim_date)
)
RETURN DIVIDE( _2020 - _Baseline, _Baseline )

-- Anomalie ratio F/H (écart vs attendu national)
[Anomalie Ratio FH] =
VAR _RatioCohorte = DIVIDE( [Nb Décès Femmes], [Nb Décès Hommes] )
VAR _RatioNational = CALCULATE(
    DIVIDE( [Nb Décès Femmes], [Nb Décès Hommes] ),
    ALL( fact_deces[annee_naissance] )
)
RETURN _RatioCohorte - _RatioNational
```

---

## Annexe — Table de paramètres recommandée

Pour les slicers dynamiques (seuil z-score, génération, année), créer une table de paramètres :

```dax
-- Table paramètre seuil z-score
Seuil Z-Score =
DATATABLE(
    "Seuil", DOUBLE, "Libellé", STRING,
    { {1.5, "Léger (|z|>1.5)"}, {2.0, "Modéré (|z|>2.0)"}, {2.5, "Fort (|z|>2.5)"}, {3.0, "Critique (|z|>3.0)"} }
)

-- Table paramètre génération
Générations =
DATATABLE(
    "Libellé",  STRING,
    "Début",    INTEGER,
    "Fin",      INTEGER,
    "Ordre",    INTEGER,
    {
        {"Génération Perdue",     1883, 1900, 1},
        {"Génération Silencieuse",1901, 1927, 2},
        {"Sil. tardive / WWII",   1928, 1945, 3},
        {"Baby-boomers",          1946, 1964, 4},
        {"Génération X",          1965, 1980, 5},
        {"Millennials",           1981, 1996, 6}
    }
)
```

---

*Document généré automatiquement — DK-BI-ListeDeces · Mai 2026*
