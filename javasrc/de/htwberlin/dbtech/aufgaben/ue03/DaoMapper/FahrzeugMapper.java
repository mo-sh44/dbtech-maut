package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.aufgaben.ue03.TableObjects.Fahrzeug;
import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

/**
 * Data-Mapper fuer die Tabelle FAHRZEUG (und Zugriff ueber Kennzeichen).
 */
public class FahrzeugMapper {

    private Connection connection;

    public FahrzeugMapper(Connection connection) {
        this.connection = connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    /**
     * Prueft, ob ein Fahrzeug im System bekannt ist.
     * Entweder als registriertes Fahrzeug oder ueber eine Buchung.
     */
    public boolean isFahrzeugBekannt(String kennzeichen) {
        // 1) Direkt in der FAHRZEUG-Tabelle nachschauen
        if (existsInFahrzeugTable(kennzeichen)) {
            return true;
        }

        // 2) Sonst ueber Buchungen (manuelles Verfahren) pruefen
        BuchungMapper buchungMapper = new BuchungMapper(getConnection());
        return buchungMapper.hasAnyBuchung(kennzeichen);
    }

    private boolean existsInFahrzeugTable(String kennzeichen) {
        String sql = "SELECT 1 FROM FAHRZEUG WHERE KENNZEICHEN = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei existsInFahrzeugTable", e);
        }
    }

    /**
     * Liefert die Achszahl fuer ein Kennzeichen.
     *  - Wenn Fahrzeug registriert ist: Achsen aus FAHRZEUG-Tabelle.
     *  - Sonst: Achsen aus einer offenen Buchung (ueber BuchungMapper),
     *            default ist der uebergebene Wert.
     */
    public int ermittleAchsen(String kennzeichen, int gemeldeteAchsen) {
        String sql = "SELECT ACHSEN FROM FAHRZEUG WHERE KENNZEICHEN = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // Achszahl direkt vom Fahrzeug
                    return rs.getInt("ACHSEN");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler beim Lesen der Achszahl aus FAHRZEUG", e);
        }

        // Wenn das Fahrzeug nicht in FAHRZEUG steht, ggf. aus einer Buchung holen
        BuchungMapper buchungMapper = new BuchungMapper(getConnection());
        return buchungMapper.getAchsenAusOffenerBuchung(kennzeichen, gemeldeteAchsen);
    }

    /**
     * Liefert ein Fahrzeug-Objekt, falls fuer das Kennzeichen ein Fahrzeuggeraet
     * (On-Board-Unit) eingetragen ist. Sonst null.
     */
    public Fahrzeug findFahrzeugMitGeraet(String kennzeichen) {
        String sql =
                "SELECT f2.* " +
                        "FROM FAHRZEUGGERAET fg " +
                        "JOIN FAHRZEUG f2 ON fg.FZ_ID = f2.FZ_ID " +
                        "WHERE f2.KENNZEICHEN = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {

                    return new Fahrzeug(
                            rs.getLong(1),
                            rs.getInt(2),
                            rs.getInt(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getInt(6),
                            rs.getInt(7),
                            rs.getDate(8),
                            rs.getDate(9),
                            rs.getString(10)
                    );
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei findFahrzeugMitGeraet", e);
        }

        return null;
    }
}
