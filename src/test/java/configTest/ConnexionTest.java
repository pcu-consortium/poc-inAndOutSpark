package configTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import config.Entree;
import config.Filtre;
import config.Format;
import config.TypeConnexion;

public class ConnexionTest {

	/**
	 * Ne teste pas le all de la requete sql
	 */
	// @Test
	public void isThereASQLRequestTest() {

		Entree e = new Entree();
		e.setFormat(Format.JSON);
		e.setNom("Test");
		e.setType(TypeConnexion.FILE);
		ArrayList<String> list = new ArrayList<>();
		list.add("Test");

		// Si le Filtre n'existe tout simplement pas
		assertFalse(e.isThereASQLRequest());

		// Si le filtre est composé d'un select simple
		e.setFiltreSQL(new Filtre("", "a", ""));
		assertTrue(e.isThereASQLRequest());

		// Si le filtre est composé d'un select et d'un where
		e.setFiltreSQL(new Filtre("a = b", "a", ""));
		assertTrue(e.isThereASQLRequest());

		// Si le filtre est composé d'un where et pas de select
		e.setFiltreSQL(new Filtre("a = b", "", ""));
		assertTrue(e.isThereASQLRequest());

	}

	/**
	 * Ne teste pas le all de la requete sql
	 */
	// @Test
	public void getRequeteSQLTest() {

		// Création des string utiles pour les tests
		String from = "colonne";
		String testSelect = "testSelect";
		String testWhere0 = "";
		String testWhere1 = "x = y";
		String testWhere2 = "x = y AND y = z";

		// Création de l'objet de test
		Entree e = new Entree();
		e.setFormat(Format.JSON);
		e.setNom("Test");
		e.setType(TypeConnexion.FILE);

		e.setFiltreSQL(new Filtre(testWhere0, testSelect, ""));

		// Test Select simple
		String test0 = "SELECT " + testSelect + " FROM colonne;";
		assertEquals(e.getRequeteSQL(from), test0);

		// Test Select *
		e.getFiltreSQL().setSelect("");
		String test1 = "SELECT * FROM colonne;";
		assertEquals(e.getRequeteSQL(from), test1);

		// Test Select simple + where simple
		e.getFiltreSQL().setSelect(testSelect);
		e.getFiltreSQL().setWhere(testWhere1);
		String test2 = "SELECT " + testSelect + " FROM colonne WHERE " + testWhere1 + ";";
		assertEquals(e.getRequeteSQL(from), test2);

		// Test select simple + where complexe
		e.getFiltreSQL().setWhere(testWhere2);
		String test3 = "SELECT " + testSelect + " FROM colonne WHERE " + testWhere2 + ";";
		assertEquals(e.getRequeteSQL(from), test3);

	}

}
