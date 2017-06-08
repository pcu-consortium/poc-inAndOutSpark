# Composition fichier de configuration
   Le fichier de configuration est séparé en 3 parties principales :
   - Les entrées : Le champ "in"
   - Les opérations : Le champs "operations"
   - Les sorties : Le champ "out"

## Les entrées
Le champs entrées est à mettre au niveau 0 du fichire de conf avec pour nom "in" et est composé d'une liste d'entrées.

Chaque entrée possède :

| Statut | Nom du champ | Description |
| :----: | :----------: | ----------- |
| OBLIGATOIRE | nom | Le nom du flux, à réutiliser dans le reste du fichier de configuration
| OBLIGATOIRE | type | type du flux d'entrée (fichier ou kafka) pour le moment n'est pas pris en compte et on lit uniquement des fichiers |
| FACULTATIF | filtreSQL | Requete SQL simple à effectuer sur le flux d'entrée |
| DEPRECATED | select | Effectue un select sur le flux |
| DEPRECATED | where | Effectue un where sur le flux |

**Note :** Si les champs select ou where sont renseignés, le champ filtreSQL n'est pas pris en compte. 

## Les opérations
Le champs opérations est à mettre au niveau 0 du fichire de conf avec pour nom "operations" et est composé d'une liste d'opérations et une autre d'opérations multi-sources.

Le champs operations possède :

| Statut | Nom du champ | Description |
| :----: | :----------: | ----------- |
| OBLIGATOIRE | nom_source | Le nom de la source sur laquelle effectuer l'opération (réutiliser le nom déclaré dans l'entrée)
| FACULTATIF | operations | La liste d'opérations à effectuer sur le flux source
| FACULTATIF | operations_multi_sources | La liste d'opérations à affectuer sur le flux source nécessitant les données de plusieurs sources

Liste des opérations existantes :

| Type opération | Nom de l'opération | Paramètres | Notes |
| -------------- | :----------------: | ---------- | ----- |
| OPERATIONS | append | [colonne1][colonne2] [nouvelleColonne] | Le champ nouvelleColonne est facultatif et a pour valeur par défaut : colonne1-colonne2 |
| OPERATIONS_MULTI_SOURCES | join | [flux1] [flux2] [colonne1] [colonne2] | Si les deux colonne ont le même nom, n'indiquer que le champ [colonne1]

**Note :** Il est possible d'appeler plusieurs fois la même opération sur le même flux.

## Les sorties
Le champs sorties est à mettre au niveau 0 du fichire de conf avec pour nom "out" et est composé d'une liste de sorties.

Chaque sortie possède :

| Statut | Nom du champ | Description |
| :----: | :----------: | ----------- |
| OBLIGATOIRE | nom | Nom du flux de sortie. Donnera le nom du dossier crée avec les résultats |
| OBLIGATOIRE | type | type du flux de sortie (fichier ou kafka). Pour le moment on ne prend en compte que les fichiers |
| FACULTATIF | from | Liste des flux à faire ressortir par cette sortie. Le nom du flux donnera le nom du sous-dossier dans lequel seront les résultats pour ce flux |

**Note :** Dans le cas de sortie sous format fichier, une arborescence est crée avec au premier niveau un dossier par sortie puis pour chaque dossier, un sous-dossier par flux d'entrées.

# Composition des fichiers d'entrées et de sortie
Les fichiers d'entrée doivent être composés d'objets JSON (1 objet JSON par ligne)
Exemple :
``` 
{"test1":"test1.1", "test2":"test2.1", "test3":"test3.1"}
{"test1":"test1.2", "test2":"test2.2", "test3":"test3.2"}
{"test1":"test1.3", "test2":"test2.3", "test3":"test3.3"}
```
Les fichiers de sorties seront écrit sous le même format

# Détails sur le code

Le code est séparé en 3 parties distinctes.

## Le pré-do
Le pré-do ne doit normalement **pas** être modifié par l'utilisateur et laissé tel que.
Le pré-do se charge de :
- L'initialisation des variables spark utilisées de manière globale
- La lecture du ficher de configuration et de sa transformation en objet java
- La lecture des données indiquées dans le fichier de configuration
- Application de(s/la) requête(s) SQL si indiquée(s) dans le fichier de configuration

## Le do
Cette partie est totalement libre de modification par l'utilisateur
Le do (encore non-décomposé) applique les filtres indiqués dans le fichier de configuration.

# Le post-do
Le post-do ne doit normalement **pas** être modifié par l'utilisateur et laissé tel que.
Le post-do se charge d'écrire les résultats en fonction des indications données dans le fichier de configuration (voir [format de sortie]( https://github.com/pcu-consortium/poc-inAndOutSpark/blob/master/README.md#les-sorties "Format de sortie" )).
