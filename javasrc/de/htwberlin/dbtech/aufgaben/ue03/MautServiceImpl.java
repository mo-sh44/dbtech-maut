package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;

/**
 * Implementierung des Maut-Services (Variante ohne DAO-Klassen).
 */
public class MautServiceImpl implements IMautService {

    private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);

    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
            throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {

        // 1) Ist das Fahrzeug bekannt?
        boolean istAutoRegistriert    = istAutoRegistriert(kennzeichen);
        boolean istManuellRegistriert = istManuellRegistriert(mautAbschnitt, kennzeichen);

        if (!istAutoRegistriert && !istManuellRegistriert) {
            throw new UnkownVehicleException(
                    "Fahrzeug " + kennzeichen + " ist weder im automatischen noch im manuellen Verfahren bekannt.");
        }

        // 2) Achszahl aus den Stamm-/Buchungsdaten ermitteln
        int gespeicherteAchsen;
        if (istAutoRegistriert) {
            // automatische Verfahren → Achsen aus FAHRZEUG
            gespeicherteAchsen = getAchsenFuerFahrzeug(kennzeichen);
        } else {
            // manuelles Verfahren → Achsen aus offener Buchung
            try {
                gespeicherteAchsen = getAchsenFuerBuchung(mautAbschnitt, kennzeichen);
            } catch (DataException e) {
                // keine offene Buchung → prüfen, ob schon abgeschlossen (Doppelbefahrung)
                if (hatAbgeschlosseneBuchung(mautAbschnitt, kennzeichen)) {
                    throw new AlreadyCruisedException(
                            "Doppelbefahrung für Abschnitt " + mautAbschnitt + " und Kennzeichen " + kennzeichen);
                } else {
                    throw e;
                }
            }
        }

        // 3) Achszahl prüfen
        if (gespeicherteAchsen != achszahl) {
            throw new InvalidVehicleDataException(
                    "Achszahl stimmt nicht mit den gespeicherten Daten überein. Erwartet: "
                            + gespeicherteAchsen + ", gemeldet: " + achszahl);
        }

        // 4) Verfahren unterscheiden
        if (istAutoRegistriert) {
            // AUTOMATISCHES VERFAHREN: Maut berechnen & Mauterhebung speichern
            fuehreAutomatischesVerfahrenAus(mautAbschnitt, achszahl, kennzeichen);
        } else {
            // MANUELLES VERFAHREN: offene Buchung auf "abgeschlossen" setzen
            entwerteBuchung(mautAbschnitt, kennzeichen);
        }
    }

    // --------------------------------------------------------------------
    // Hilfsmethoden – Registrierung / Achsen / Buchungen
    // --------------------------------------------------------------------

    /** Fahrzeug im automatischen Verfahren bekannt (Fahrzeug + Gerät, nicht abgemeldet)? */
    private boolean istAutoRegistriert(String kennzeichen) {
        String sql =
                "SELECT f.FZ_ID " +
                        "FROM Fahrzeug f " +
                        "JOIN Fahrzeuggerat fg ON f.FZ_ID = fg.FZ_ID " +
                        "WHERE f.Kennzeichen = ? " +
                        "  AND f.Abmeldedatum IS NULL";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setString(1, kennzeichen);
            try (ResultSet rs = s.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei istAutoRegistriert", e);
        }
    }

    /** Gibt es irgendeine Buchung für dieses Kennzeichen + Abschnitt? */
    private boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen) {
        String sql =
                "SELECT 1 FROM Buchung " +
                        "WHERE Kennzeichen = ? AND Abschnitts_id = ?";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setString(1, kennzeichen);
            s.setInt(2, mautAbschnitt);
            try (ResultSet rs = s.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei istManuellRegistriert", e);
        }
    }

    /** Achszahl im automatischen Verfahren aus FAHRZEUG. */
    private int getAchsenFuerFahrzeug(String kennzeichen) {
        String sql =
                "SELECT Achsen FROM Fahrzeug " +
                        "WHERE Kennzeichen = ? AND Abmeldedatum IS NULL";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setString(1, kennzeichen);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("Achsen");
                } else {
                    throw new DataException("Fahrzeug mit Kennzeichen " + kennzeichen + " nicht gefunden.");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getAchsenFuerFahrzeug", e);
        }
    }

    /**
     * Achszahl im manuellen Verfahren:
     * über offene Buchung → MAUTKATEGORIE.ACHSZAHL (String wie '= 4', '>= 5') → nur Ziffern extrahieren.
     */
    private int getAchsenFuerBuchung(int mautAbschnitt, String kennzeichen) {
        String sql =
                "SELECT k.ACHSZAHL " +
                        "FROM Buchung b " +
                        "JOIN Mautkategorie k ON b.Kategorie_id = k.Kategorie_id " +
                        "WHERE b.Abschnitts_id = ? " +
                        "  AND b.Kennzeichen   = ? " +
                        "  AND b.B_Id          = 1";     // 1 = offen

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setInt(1, mautAbschnitt);
            s.setString(2, kennzeichen);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    String achsenString = rs.getString(1); // z.B. "= 4", ">= 5"
                    if (achsenString == null) {
                        throw new DataException("ACHSZAHL ist null in Mautkategorie.");
                    }
                    String nurZahlen = achsenString.replaceAll("[^0-9]", "");
                    if (nurZahlen.isEmpty()) {
                        throw new DataException("ACHSZAHL enthält keine Ziffern: " + achsenString);
                    }
                    return Integer.parseInt(nurZahlen);
                } else {
                    throw new DataException(
                            "Keine offene Buchung für " + kennzeichen + " auf Abschnitt " + mautAbschnitt + " gefunden.");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getAchsenFuerBuchung", e);
        }
    }

    /** Gibt es bereits eine abgeschlossene Buchung für diesen Abschnitt + Kennzeichen? */
    private boolean hatAbgeschlosseneBuchung(int mautAbschnitt, String kennzeichen) {
        String sql =
                "SELECT 1 FROM Buchung " +
                        "WHERE Abschnitts_id = ? " +
                        "  AND Kennzeichen   = ? " +
                        "  AND B_Id          = 3";      // 3 = abgeschlossen

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setInt(1, mautAbschnitt);
            s.setString(2, kennzeichen);
            try (ResultSet rs = s.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei hatAbgeschlosseneBuchung", e);
        }
    }

    /** Setzt offene Buchung auf "abgeschlossen". */
    private void entwerteBuchung(int mautAbschnitt, String kennzeichen) {
        String sql =
                "UPDATE Buchung " +
                        "SET B_Id = 3 " +
                        "WHERE Abschnitts_id = ? " +
                        "  AND Kennzeichen   = ? " +
                        "  AND B_Id          = 1";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setInt(1, mautAbschnitt);
            s.setString(2, kennzeichen);
            int updated = s.executeUpdate();
            if (updated == 0) {
                throw new DataException("Keine offene Buchung zum Entwerten gefunden.");
            }
            L.info("Buchung für Fahrzeug {} auf 'abgeschlossen' gesetzt", kennzeichen);
        } catch (SQLException e) {
            throw new DataException("Fehler bei entwerteBuchung", e);
        }
    }

    // --------------------------------------------------------------------
    // Hilfsmethoden – automatisches Verfahren
    // --------------------------------------------------------------------

    /** FZ_ID des Fahrzeugs mit Gerät. */
    private long getFahrzeugIdMitGeraet(String kennzeichen) {
        String sql =
                "SELECT f.FZ_ID " +
                        "FROM Fahrzeug f " +
                        "JOIN Fahrzeuggerat fg ON f.FZ_ID = fg.FZ_ID " +
                        "WHERE f.Kennzeichen = ? " +
                        "  AND f.Abmeldedatum IS NULL";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setString(1, kennzeichen);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("FZ_ID");
                } else {
                    throw new DataException("Fahrzeug mit Gerät nicht gefunden: " + kennzeichen);
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getFahrzeugIdMitGeraet", e);
        }
    }

    /** Länge des Mautabschnitts. */
    private double getAbschnittsLaenge(int mautAbschnitt) {
        String sql = "SELECT LAENGE FROM Mautabschnitt WHERE Abschnitts_id = ?";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setInt(1, mautAbschnitt);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    return rs.getDouble(1);
                } else {
                    throw new DataException("Mautabschnitt " + mautAbschnitt + " nicht gefunden.");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getAbschnittsLaenge", e);
        }
    }

    /** Kleine Hilfeklasse für Tarif-Info. */
    private static class TarifInfo {
        final int kategorieId;
        final double mautJeKm;
        TarifInfo(int kategorieId, double mautJeKm) {
            this.kategorieId = kategorieId;
            this.mautJeKm = mautJeKm;
        }
    }

    /** Tarif anhand Kennzeichen (Schadstoffklasse) + Achsenzahl bestimmen. */
    private TarifInfo getTarifFuerFahrzeug(String kennzeichen, int achsen) {

        int effektiveAchsen = Math.min(achsen, 5); // Sammelkategorie ab 5 Achsen

        String sql =
                "SELECT k.Kategorie_id, k.Mautsatz_je_km " +
                        "FROM Fahrzeug f " +
                        "JOIN Mautkategorie k ON f.SSKL_ID = k.SSKL_ID " +
                        "WHERE f.Kennzeichen = ? " +
                        "  AND f.Abmeldedatum IS NULL " +
                        "  AND k.ACHSZAHL LIKE ?";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setString(1, kennzeichen);
            s.setString(2, "%" + effektiveAchsen);   // ACHSZAHL z.B. "= 4" → LIKE "%4"
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    int katId = rs.getInt("Kategorie_id");
                    double mautJeKm = rs.getDouble("Mautsatz_je_km");
                    return new TarifInfo(katId, mautJeKm);
                } else {
                    throw new DataException("Keine passende Mautkategorie gefunden.");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getTarifFuerFahrzeug", e);
        }
    }

    /** nächste freie MAUT_ID bestimmen. */
    private int getNaechsteMautId() {
        String sql = "SELECT MAX(MAUT_ID) FROM Mauterhebung";

        try (PreparedStatement s = getConnection().prepareStatement(sql);
             ResultSet rs = s.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1) + 1;
            } else {
                return 1;
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei getNaechsteMautId", e);
        }
    }

    // Führt das automatische Verfahren durch: Maut berechnen & Mauterhebung speichern.
    private void fuehreAutomatischesVerfahrenAus(int mautAbschnitt, int achszahl, String kennzeichen) {

        long fzId   = getFahrzeugIdMitGeraet(kennzeichen);
        double laenge = getAbschnittsLaenge(mautAbschnitt);
        TarifInfo tarif = getTarifFuerFahrzeug(kennzeichen, achszahl);

        // Debug-Ausgabe, damit نعرف شو عم يصير
        L.info("DEBUG: laenge={}, mautJeKm={}", laenge, tarif.mautJeKm);

        double kosten = (laenge * tarif.mautJeKm) / 100000.0;

        // nächste freie MAUT_ID bestimmen
        int neueMautId = getNaechsteMautId();

        String sql =
                "INSERT INTO Mauterhebung " +
                        "  (MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, KOSTEN, BEFAHRUNGSDATUM) " +
                        "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement s = getConnection().prepareStatement(sql)) {
            s.setInt(1, neueMautId);
            s.setInt(2, mautAbschnitt);
            s.setLong(3, fzId);
            s.setInt(4, tarif.kategorieId);
            s.setDouble(5, kosten);
            s.setDate(6, Date.valueOf(LocalDate.now()));
            s.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("Fehler beim Speichern der Mauterhebung im automatischen Verfahren", e);
        }
    }

}
