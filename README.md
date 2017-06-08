# Composition fichier de configuration
   Le fichier de configuration est séparé en 3 parties principales :
   - Les entrées : Le champ "in"
   - Les opérations : Le champs "operations"
   - Les sorties : Le champ "out"

## Les entrées
Le champs entrées est à mettre au niveau 0 du fichire de conf avec pour nom "in" et est composé d'une liste d'entrées.

Chaque entrée possède :
| Statut | Nom du champ | Description |
| ------ | ------ | ------ |
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
| ------ | ------ | ------ |
| OBLIGATOIRE | nom_source | Le nom de la source sur laquelle effectuer l'opération (réutiliser le nom déclaré dans l'entrée)
| FACULTATIF | operations | La liste d'opérations à effectuer sur le flux source
| FACULTATIF | operations_multi_sources | La liste d'opérations à affectuer sur le flux source nécessitant les données de plusieurs sources

Liste des opérations existantes :
| Type opération | Nom de l'opération | Paramètres | Notes |
| ------ | ------ | ------ | ------ |
| OPERATIONS | append | [colonne1][colonne2] [nouvelleColonne] | Le champ nouvelleColonne est facultatif et a pour valeur par défaut : colonne1-colonne2 |
| OPERATIONS_MULTI_SOURCES | join | [flux1] [flux2] [colonne1] [colonne2] | Si les deux colonne ont le même nom, n'indiquer que le champ [colonne1]
**Note :** Il est possible d'appeler plusieurs fois la même opération sur le même flux.

## Les sorties
Le champs sorties est à mettre au niveau 0 du fichire de conf avec pour nom "out" et est composé d'une liste de sorties.

Chaque sortie possède :
| Statut | Nom du champ | Description |
| ------ | ------ | ------ |
| OBLIGATOIRE | nom | Nom du flux de sortie. Donnera le nom du dossier crée avec les résultats |
| OBLIGATOIRE | type | type du flux de sortie (fichier ou kafka). Pour le moment on ne prend en compte que les fichiers |
| FACULTATIF | from | Liste des flux à faire ressortir par cette sortie. Le nom du flux donnera le nom du sous-dossier dans lequel seront les résultats pour ce flux |
**Note :** Dans le cas de sortie sous format fichier, une arborescence est crée avec au premier niveau un dossier par sortie puis pour chaque dossier, un sous-dossier par flux d'entrées.
