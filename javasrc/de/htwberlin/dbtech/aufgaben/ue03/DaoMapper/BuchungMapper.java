package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

public class BuchungMapper {
    private static final int STATUS_OFFEN = 1;
    private static final int STATUS_ABGESCHLOSSEN = 3;

    private Connection connection;

    public BuchungMapper(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection nicht gesetzt");
        }
        return connection;
    }

    /**
     * Liefert die ID der offenen Buchung fuer ein Kennzeichen (B_ID = 1).
     */
    public Integer findOffeneBuchungId(String kennzeichen) {
        final String sql = "SELECT BUCHUNG_ID FROM BUCHUNG WHERE KENNZEICHEN = ? AND B_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            ps.setInt(2, STATUS_OFFEN);
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
     * Liest die Achszahl aus der Mautkategorie einer offenen Buchung.
     * Wird fuer die Achsen-Validierung im manuellen Verfahren genutzt.
     */
    public int getAchsenFuerOffeneBuchung(String kennzeichen) {
        final String sql =
                "SELECT m.ACHSZAHL FROM BUCHUNG b JOIN MAUTKATEGORIE m ON b.KATEGORIE_ID = m.KATEGORIE_ID " +
                        "WHERE b.KENNZEICHEN = ? AND b.B_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            ps.setInt(2, STATUS_OFFEN);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String achsenString = rs.getString("ACHSZAHL");
                    String nurZahlen = achsenString.replaceAll("[^0-9]", "");
                    if (!nurZahlen.isEmpty()) {
                        return Integer.parseInt(nurZahlen);
                    }
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler beim Abrufen der Achsen aus Buchung.", e);
        }
        throw new DataException("Keine gültigen Achsendaten für offene Buchung gefunden.");
    }

    /**
     * Prueft, ob fuer diesen Abschnitt bereits eine abgeschlossene Buchung vorliegt (Doppelbefahrung).
     */
    public boolean istDoppelbefahrungAbgeschlossen(int mautAbschnitt, String kennzeichen) {
        final String sql = "SELECT 1 FROM BUCHUNG WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ? AND B_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            ps.setInt(2, mautAbschnitt);
            ps.setInt(3, STATUS_ABGESCHLOSSEN);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei der Doppelbefahrungsprüfung (Manuell).", e);
        }
    }

    /**
     * Setzt eine Buchung auf 'abgeschlossen' (B_ID = 3) und trägt das Befahrungsdatum ein.
     */
    public void markiereBuchungAlsAbgeschlossen(int buchungId) {
        final String sql =
                "UPDATE BUCHUNG SET B_ID = ?, BEFAHRUNGSDATUM = ? WHERE BUCHUNG_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, STATUS_ABGESCHLOSSEN);
            ps.setDate(2, Date.valueOf(LocalDate.now()));
            ps.setInt(3, buchungId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("Fehler beim Abschliessen der Buchung.", e);
        }
    }
}