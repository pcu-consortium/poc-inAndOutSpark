package config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Thomas Estrabaud - Smile
 * 
 *         Classe représentant la sortie dans spark
 *
 */
public class Sortie extends Connexion {

	List<String> from;
	String key;

	/**
	 * 
	 */
	public Sortie() {
		super();
		from = new ArrayList<String>();
		index = "";
		key = "";
	}

	/**
	 * @param nom
	 * @param type
	 * @param format
	 * @param from
	 */
	public Sortie(String nom, TypeConnexion type, Format format, List<String> from, String index, String key) {
		super();
		this.nom = nom;
		this.type = type;
		this.format = format;
		this.from = from;
		this.index = index;
		this.key = key;
	}

	/**
	 * @return
	 */
	public List<String> getFrom() {
		return from;
	}

	/**
	 * @param from
	 */
	public void setFrom(List<String> from) {
		this.from = from;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Sortie " + nom + " " + type.name();
	}

}
