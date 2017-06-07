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

	/**
	 * 
	 */
	public Sortie() {
		super();
		from = new ArrayList<String>();
	}

	/**
	 * @param nom
	 * @param type
	 * @param format
	 * @param from
	 */
	public Sortie(String nom, TypeConnexion type, Format format, List<String> from) {
		super();
		this.nom = nom;
		this.type = type;
		this.format = format;
		this.from = from;
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
