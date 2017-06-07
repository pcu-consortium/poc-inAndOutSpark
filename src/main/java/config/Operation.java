package config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Thomas Estrabaud - Smile
 * 
 *         Classe représentant une opération (petite modif de la donnée)
 *
 */
public class Operation implements Serializable {

	String nom_source;
	List<String> operations;
	List<String> operations_multi_sources;

	/**
	 * @return
	 */
	public String getNom_source() {
		return nom_source;
	}

	/**
	 * @param nom_source
	 */
	public void setNom_source(String nom_source) {
		this.nom_source = nom_source;
	}

	/**
	 * @return
	 */
	public List<String> getOperations() {
		return operations;
	}

	/**
	 * @param operations
	 */
	public void setOperations(List<String> operations) {
		this.operations = operations;
	}

	public List<String> getOperations_multi_sources() {
		return operations_multi_sources;
	}

	public void setOperations_multi_sources(List<String> operations_multi_sources) {
		this.operations_multi_sources = operations_multi_sources;
	}

	/**
	 * @param nom_source
	 * @param operations
	 */
	public Operation(String nom_source, List<String> operations, List<String> operations_multi_sources) {
		super();
		this.nom_source = nom_source;
		this.operations = operations;
		this.operations_multi_sources = operations_multi_sources;
	}

	/**
	 * 
	 */
	public Operation() {
		super();
		nom_source = "";
		operations = new ArrayList<String>();
		operations_multi_sources = new ArrayList<String>();
	}

	@Override
	public String toString() {
		return nom_source + " " + operations;
	}

}
