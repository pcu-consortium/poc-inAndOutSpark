package config;

import java.io.Serializable;

/**
 * Classe Filtre,pour le moment correspond à un filtre SQL (une requete Select
 * from where), il sera peut etre nécessaire de rajouter d'autres types de
 * filtres).
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class Filtre implements Serializable {

	String where;
	String select;
	String all;

	/**
	 * Constructeur par défaut
	 * 
	 * @param where
	 * @param select
	 */
	public Filtre(String where, String select, String all) {
		super();
		this.where = where;
		this.select = select;
		this.all = all;
	}

	/**
	 * 
	 */
	public Filtre() {
		super();
		where = "";
		select = "";
		all = "";
	}

	/**
	 * @return
	 */
	public String getWhere() {
		return where;
	}

	/**
	 * @param where
	 */
	public void setWhere(String where) {
		this.where = where;
	}

	/**
	 * @return
	 */
	public String getSelect() {
		return select;
	}

	/**
	 * @param select
	 */
	public void setSelect(String select) {
		this.select = select;
	}

	/**
	 * @return
	 */
	public String getAll() {
		return all;
	}

	/**
	 * @param all
	 */
	public void setAll(String all) {
		this.all = all;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "";
		if (all != "")
			s += all + " ";
		if (select != "")
			s += select + " ";
		if (where != "")
			s += where;
		if (s.endsWith(" "))
			return s.substring(0, s.length() - 1);
		return s;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((all == null) ? 0 : all.hashCode());
		result = prime * result + ((select == null) ? 0 : select.hashCode());
		result = prime * result + ((where == null) ? 0 : where.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Filtre other = (Filtre) obj;
		if (all == null) {
			if (other.all != null)
				return false;
		} else if (!all.equals(other.all))
			return false;
		if (select == null) {
			if (other.select != null)
				return false;
		} else if (!select.equals(other.select))
			return false;
		if (where == null) {
			if (other.where != null)
				return false;
		} else if (!where.equals(other.where))
			return false;
		return true;
	}

	/**
	 * Renvoie vrai s'il y a une requete à effectuer, faux sinon (à ne pas
	 * utiliser directement, à appeler depuis Connexion)
	 * 
	 * @see Connexion#isThereASQLRequest()
	 * 
	 * @return
	 */
	public boolean isThereARequest() {
		if (this.select == "" && this.where == "" && this.all == "")
			return false;
		else
			return true;
	}

}
