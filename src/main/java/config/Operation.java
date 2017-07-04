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

	String input_source;
	List<String> processors;
	String output_source;

	/**
	 * @return
	 */
	public String getInput_source() {
		return input_source;
	}

	/**
	 * @param nom_source
	 */
	public void setInput_source(String input_source) {
		this.input_source = input_source;
		if (output_source == "")
			this.output_source = this.input_source;
	}

	/**
	 * @return
	 */
	public List<String> getProcessors() {
		return processors;
	}

	/**
	 * @param operations
	 */
	public void setProcessors(List<String> processors) {
		this.processors = processors;
	}

	/**
	 * @return
	 */
	public String getOutput_source() {
		return output_source;
	}

	/**
	 * @param output_source
	 */
	public void setOutput_source(String output_source) {
		this.output_source = output_source;
	}

	/**
	 * @return
	 */
	public boolean isOutputSameAsInput() {

		return this.input_source == this.output_source;
	}

	/**
	 * @param nom_source
	 * @param operations
	 */
	public Operation(String Input_source, List<String> processors, String output_source) {
		super();
		this.input_source = Input_source;
		this.processors = processors;
		this.output_source = output_source;
	}

	/**
	 * 
	 */
	public Operation() {
		super();
		input_source = "";
		processors = new ArrayList<String>();
		output_source = "";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return input_source + " " + processors;
	}

}
