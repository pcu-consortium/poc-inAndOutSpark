package config;

import java.io.Serializable;

/**
 * @author Thomas Estrabaud - Smile Classe mère de Entree et Sortie. Représente
 *         les entrées et les sorties du spark
 */
public abstract class Connexion implements Serializable {
   private static final long serialVersionUID = 6922437965138381068L;
   
   String nom;
	TypeConnexion type;
	Format format;
	String ipBrokers;
	String topic;
	String index;
	/** in KAFKA_STREAM only */
   private String startingOffsets;
   /** out ES only */
   private boolean keepOriginal = true;

	/**
	 * @return
	 */
	public String getNom() {
		return nom;
	}

	/**
	 * @param nom
	 */
	public void setNom(String nom) {
		this.nom = nom;
	}

	/**
	 * @return
	 */
	public TypeConnexion getType() {
		return type;
	}

	/**
	 * @param type
	 */
	public void setType(TypeConnexion type) {
		this.type = type;
	}

	/**
	 * 
	 * @return
	 */
	public Format getFormat() {
		return format;
	}

	/**
	 * @param format
	 */
	public void setFormat(Format format) {
		this.format = format;
	}

	/**
	 * @return
	 */
	public String getIpBrokers() {
		return ipBrokers;
	}

	public void setIpBrokers(String ipBrokers) {
		this.ipBrokers = ipBrokers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * @return
	 */
	public String getIndex() {
		return index;
	}

	/**
	 * @param index
	 */
	public void setIndex(String index) {
		this.index = index;
	}

   public String getStartingOffsets() {
      return startingOffsets;
   }
   public void setStartingOffsets(String startingOffsets) {
      this.startingOffsets = startingOffsets;
   }

   public boolean isKeepOriginal() {
      return keepOriginal;
   }
   public void setKeepOriginal(boolean keepOriginal) {
      this.keepOriginal = keepOriginal;
   }

	/**
	 * Constructeur avec tous les champs de connexion
	 * 
	 * @param nom
	 * @param type
	 * @param format
	 * @param filtreSQL
	 */
	public Connexion(String nom, TypeConnexion type, Format format, Filtre filtreSQL, String ipbrokers, String topic) {
		super();
		this.nom = nom;
		this.type = type;
		this.format = format;
		this.ipBrokers = ipbrokers;
		this.topic = topic;
	}

	/**
	 * Constructeur par défaut
	 */
	public Connexion() {
		super();
		nom = "";
		type = TypeConnexion.OTHER;
		format = Format.JSON;
		ipBrokers = "";
		topic = "";
		index = "";

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
		result = prime * result + ((format == null) ? 0 : format.hashCode());
		result = prime * result + ((nom == null) ? 0 : nom.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		Connexion other = (Connexion) obj;
		if (format != other.format)
			return false;
		if (nom == null) {
			if (other.nom != null)
				return false;
		} else if (!nom.equals(other.nom))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

}
