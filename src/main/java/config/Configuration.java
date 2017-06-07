package config;

import java.util.ArrayList;
import java.util.List;

import scala.Serializable;

/**
 * Objet correspondant à la config yaml /!\ il faut que chacun des classes qui
 * sont comprises dans celle ci aient : un constructeur par défaut, leurs
 * getters/setters
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class Configuration implements Serializable {

	List<Entree> in;
	List<Sortie> out;
	List<Operation> operations;

	public Configuration() {
		super();
		this.in = new ArrayList<Entree>();
		this.out = new ArrayList<Sortie>();
		this.operations = new ArrayList<Operation>();
	}

	/**
	 * @return
	 */
	public List<Entree> getIn() {
		return in;
	}

	/**
	 * @param inFiles
	 */
	public void setIn(List<Entree> inFiles) {
		this.in = inFiles;
	}

	/**
	 * @return
	 */
	public List<Sortie> getOut() {
		return out;
	}

	/**
	 * @param outFiles
	 */
	public void setOut(List<Sortie> outFiles) {
		this.out = outFiles;
	}

	/**
	 * @return
	 */
	public List<Operation> getOperations() {
		return operations;
	}

	/**
	 * @param operations
	 */
	public void setOperations(List<Operation> operations) {
		this.operations = operations;
	}

	/**
	 * @param inFiles
	 * @param outFiles
	 * @param operations
	 */
	public Configuration(List<Entree> inFiles, List<Sortie> outFiles, List<Operation> operations) {
		super();
		this.in = inFiles;
		this.out = outFiles;
		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return in.toString() + " " + out.toString() + " " + operations.toString();
		/*
		 * String res = "in = ["; for (int i = 0; i < inFiles.size(); i++) res
		 * += inFiles.get(i).toString(); res += "] - out = ["; for (int i = 0; i
		 * < outFiles.size(); i++) res += outFiles.get(i).toString(); return
		 * res;
		 */
	}

	/**
	 * @return
	 */
	public String test() {
		return "hahaha";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((in == null) ? 0 : in.hashCode());
		result = prime * result + ((operations == null) ? 0 : operations.hashCode());
		result = prime * result + ((out == null) ? 0 : out.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Configuration other = (Configuration) obj;
		if (in == null) {
			if (other.in != null)
				return false;
		} else if (!in.equals(other.in))
			return false;
		if (operations == null) {
			if (other.operations != null)
				return false;
		} else if (!operations.equals(other.operations))
			return false;
		if (out == null) {
			if (other.out != null)
				return false;
		} else if (!out.equals(other.out))
			return false;
		return true;
	}

}
