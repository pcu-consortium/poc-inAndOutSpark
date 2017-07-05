package config;

/**
 * @author Thomas Estrabaud - Smile
 * 
 *         Classe représentant l'entrée dans Spark
 *
 */
public class Entree extends Connexion {

	Filtre filtreSQL;
	String request;

	/**
	 * 
	 */
	public Entree() {
		super();
		filtreSQL = new Filtre();
		request = "";
	}

	/**
	 * @param nom
	 * @param type
	 * @param format
	 * @param filtreSQL
	 */
	public Entree(String nom, TypeConnexion type, Format format, Filtre filtreSQL, String request) {
		this.nom = nom;
		this.type = type;
		this.format = format;
		this.filtreSQL = filtreSQL;
		this.request = request;
	}

	/**
	 * @return
	 */
	public Filtre getFiltreSQL() {
		return filtreSQL;
	}

	/**
	 * @param filtreSQL
	 */
	public void setFiltreSQL(Filtre filtreSQL) {
		this.filtreSQL = filtreSQL;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return nom + " " + type.name() + " " + filtreSQL.toString();
	}

	/**
	 * Renvoie vrai s'il y a une requete (quelque chose dans le select ou le
	 * where)
	 * 
	 * @return
	 */
	public boolean isThereASQLRequest() {

		return this.filtreSQL.isThereARequest();
	}

	/**
	 * Renvoie la requete paramétrée depuis le fichier de configuration
	 * 
	 * @param from
	 *            Valeur à mettre dans le FROM de la requête
	 * @return
	 */
	public String getRequeteSQL(String from) {

		if (filtreSQL.getAll() != "")
			return filtreSQL.getAll();

		// On met forcément un select en début de requete
		String requete = "SELECT ";
		// S'il n'y a pas de colonne spécifique à select, on fait un select *,
		// sinon on indique les colonnes qu'on veut.
		if (filtreSQL.getSelect() != "")
			requete += filtreSQL.getSelect();
		else
			requete += "*";

		// On rajoute le from sur toutes les requetes
		requete += " FROM " + from;

		// Si le where est vide
		if (filtreSQL.getWhere() == "")
			return requete + ";";
		else // Si le where n'est pas vide
			return requete + " WHERE " + filtreSQL.getWhere() + ";";
	}

}