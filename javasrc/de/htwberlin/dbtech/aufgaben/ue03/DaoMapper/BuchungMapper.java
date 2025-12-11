package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;
import java.time.LocalDate;

/**
 * Mapper fuer BUCHUNG und BUCHUNGSTATUS.
 */
public class BuchungMapper {

    private Connection connection;

    public BuchungMapper(Connection connection) {
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
     * Liest die Achszahl aus einer offenen Buchung fuer ein Kennzeichen.
     * Falls keine passende Buchung gefunden wird, wird der uebergebene Default-Wert
     * zurueckgegeben.
     */
    public int getAchsenAusOffenerBuchung(String kennzeichen, int defaultAchsen) {
        String sql =
                "SELECT m.ACHSZAHL " +
                        "FROM BUCHUNGSTATUS bs " +
                        "JOIN BUCHUNG b ON bs.B_ID = b.B_ID " +
                        "JOIN MAUTKATEGORIE m ON b.KATEGORIE_ID = m.KATEGORIE_ID " +
                        "WHERE b.KENNZEICHEN = ? AND bs.STATUS = 'offen'";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String achsText = rs.getString("ACHSZAHL");
                    // مثال: wenn der Wert wie "K3" gespeichert ist -> letzte Ziffer
                    if (achsText != null && !achsText.isEmpty()) {
                        char last = achsText.charAt(achsText.length() - 1);
                        if (Character.isDigit(last)) {
                            int achsen = Character.getNumericValue(last);
                            // ab 5 Achsen gilt i.d.R. ">=5" -> dann Default nehmen
                            return (achsen >= 5) ? defaultAchsen : achsen;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler beim Lesen der Achszahl aus Buchung", e);
        }

        return defaultAchsen;
    }

    /**
     * Prueft, ob ueberhaupt eine Buchung fuer das Kennzeichen vorhanden ist.
     */
    public boolean hasAnyBuchung(String kennzeichen) {
        String sql =
                "SELECT 1 " +
                        "FROM BUCHUNG b " +
                        "WHERE b.KENNZEICHEN = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei hasAnyBuchung", e);
        }
    }

    /**
     * Liefert die ID einer offenen Buchung fuer das Kennzeichen oder null,
     * falls keine offene Buchung existiert.
     */
    public Integer findOffeneBuchungId(String kennzeichen) {
        String sql =
                "SELECT b.BUCHUNG_ID " +
                        "FROM BUCHUNG b " +
                        "JOIN BUCHUNGSTATUS bs ON b.B_ID = bs.B_ID " +
                        "WHERE b.KENNZEICHEN = ? AND bs.STATUS = 'offen'";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("BUCHUNG_ID");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei findOffeneBuchungId", e);
        }

        return null;
    }

    /**
     * Setzt eine Buchung auf 'abgeschlossen' und traegt das heutige Befahrungsdatum ein.
     */
    public void markiereBuchungAlsAbgeschlossen(int buchungId) {
        String sql =
                "UPDATE BUCHUNG " +
                        "SET B_ID = 3, " +            // TODO: 3 durch passenden Status-Wert ersetzen
                        "    BEFAHRUNGSDATUM = ? " +
                        "WHERE BUCHUNG_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setDate(1, Date.valueOf(LocalDate.now()));
            ps.setInt(2, buchungId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("Fehler beim Aktualisieren der Buchung", e);
        }
    }

    /**
     * Prueft, ob die letzte offene Buchung fuer dieses Kennzeichen denselben
     * Mautabschnitt hat (moegliche Doppelbefahrung).
     *
     * @return true, wenn Doppelbefahrung (gleicher Abschnitt), sonst false
     */
    public boolean istDoppelbefahrungGleicherAbschnitt(int mautAbschnitt, String kennzeichen) {
        String sql =
                "SELECT b.ABSCHNITTS_ID " +
                        "FROM BUCHUNG b " +
                        "JOIN MAUTKATEGORIE m ON b.KATEGORIE_ID = m.KATEGORIE_ID " +
                        "WHERE b.KENNZEICHEN = ? AND b.B_ID = 1 " +  // TODO: Status-ID anpassen
                        "ORDER BY b.BUCHUNG_ID DESC";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int letzterAbschnitt = rs.getInt("ABSCHNITTS_ID");
                    return letzterAbschnitt == mautAbschnitt;
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei istDoppelbefahrungGleicherAbschnitt", e);
        }

        return false;
    }
}
