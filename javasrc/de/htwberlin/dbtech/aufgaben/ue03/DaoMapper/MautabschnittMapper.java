package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;
import java.time.LocalDate;

/**
 * Mapper fuer Mautabschnitte und automatische Mauterhebung.
 */
public class MautabschnittMapper {

    private Connection connection;

    public MautabschnittMapper(Connection connection) {
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
     * Öffentliche Methode, die vom MautServiceImpl aufgerufen wird.
     * Entspricht der Vorgabe aus der Lösung deines Freundes.
     */
    public void FahrtVerbuchen(int abschnittId, int achszahl, String kennzeichen) {
        verbucheAutomatischeFahrt(abschnittId, achszahl, kennzeichen);
    }

    /**
     * Bucht eine automatische Fahrt mit On-Board-Geraet:
     *  - liest Schadstoffklasse und Fahrzeug-ID
     *  - bestimmt Mautkategorie und Satz pro km
     *  - liest Laenge des Abschnitts
     *  - berechnet Kosten und legt einen Eintrag in MAUTERHEBUNG an
     */
    public void verbucheAutomatischeFahrt(int abschnittId, int achszahl, String kennzeichen) {
        try {
            // 1) Fahrzeug-Daten ermitteln (Schadstoffklasse + FZG_ID/FZ_ID)
            FahrzeugInfo fahrzeugInfo = ladeFahrzeugInfo(kennzeichen);

            if (fahrzeugInfo == null) {
                throw new DataException("Fahrzeug mit Kennzeichen " + kennzeichen +
                        " nicht gefunden oder ohne Geraet");
            }

            int effektiveAchsen = Math.min(achszahl, 5); // Kategorien sind bis ">=5"

            // 2) Mautkategorie und Satz je km bestimmen
            MautkategorieInfo kategorieInfo =
                    ladeMautkategorie(fahrzeugInfo.schadstoffklasseId(), effektiveAchsen);

            if (kategorieInfo == null) {
                throw new DataException("Keine Mautkategorie fuer SSKL_ID=" +
                        fahrzeugInfo.schadstoffklasseId() + " und Achszahl=" + effektiveAchsen + " gefunden");
            }

            // 3) Laenge des Abschnitts (in Metern)
            int laenge = ladeAbschnittsLaenge(abschnittId);

            // 4) Kosten berechnen
            // laenge in m, Satz in Cent/km -> Euro = (m * Cent/km) / (100 * 1000)
            double kosten = (laenge * kategorieInfo.mautsatzJeKm()) / 100_000.0;

            // 5) neue MAUT_ID bestimmen
            int mautId = ermittleNaechsteMautId();

            // 6) Eintrag in MAUTERHEBUNG einfügen
            fuegeMauterhebungEin(
                    mautId,
                    abschnittId,
                    fahrzeugInfo.fahrzeugId(),
                    kategorieInfo.kategorieId(),
                    kosten
            );

        } catch (SQLException e) {
            throw new DataException("Fehler beim Verbuchen der Fahrt", e);
        }
    }

    // ========= Helper-Methoden =========

    private FahrzeugInfo ladeFahrzeugInfo(String kennzeichen) throws SQLException {
        String sql =
                "SELECT f.SSKL_ID, fg.FZ_ID " +  // ggf. FZ_ID/FZG_ID anpassen je nach Schema
                        "FROM Fahrzeug f " +
                        "JOIN Fahrzeuggerat fg ON f.FZ_ID = fg.FZ_ID " +
                        "WHERE f.Kennzeichen = ? " +
                        "AND f.Abmeldedatum IS NULL";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, kennzeichen);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int ssklId = rs.getInt(1);
                    long fzgId = rs.getLong(2);
                    return new FahrzeugInfo(fzgId, ssklId);
                }
            }
        }
        return null;
    }

    private MautkategorieInfo ladeMautkategorie(int ssklId, int achsen) throws SQLException {
        String sql =
                "SELECT KATEGORIE_ID, MAUTSATZ_JE_KM " +
                        "FROM MAUTKATEGORIE " +
                        "WHERE SSKL_ID = ? AND ACHSZAHL LIKE ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, ssklId);
            ps.setString(2, "%" + achsen); // z.B. 'K3', 'A4', '>=5' etc.
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int kategorieId = rs.getInt("KATEGORIE_ID");
                    double satz = rs.getDouble("MAUTSATZ_JE_KM");
                    return new MautkategorieInfo(kategorieId, satz);
                }
            }
        }
        return null;
    }

    private int ladeAbschnittsLaenge(int abschnittId) throws SQLException {
        String sql = "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, abschnittId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("LAENGE");
                }
            }
        }
        throw new DataException("Mautabschnitt " + abschnittId + " nicht gefunden");
    }

    private int ermittleNaechsteMautId() throws SQLException {
        String sql = "SELECT COALESCE(MAX(MAUT_ID), 0) FROM MAUTERHEBUNG";

        try (PreparedStatement ps = getConnection().prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1) + 1;
        }
    }

    private void fuegeMauterhebungEin(int mautId,
                                      int abschnittId,
                                      long fahrzeugId,
                                      int kategorieId,
                                      double kosten) throws SQLException {
        // Spalten entsprechend deinem Schema:
        // MAUT_ID, ABSCHNITTS_ID, FZG_ID/FZ_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN
        String sql =
                "INSERT INTO MAUTERHEBUNG " +
                        "(MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
                        "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, mautId);
            ps.setInt(2, abschnittId);
            ps.setLong(3, fahrzeugId);
            ps.setInt(4, kategorieId);
            ps.setDate(5, Date.valueOf(LocalDate.now()));
            ps.setDouble(6, kosten);
            ps.executeUpdate();
        }
    }

    // Kleine record-Typen fuer Rueckgabewerte
    private record FahrzeugInfo(long fahrzeugId, int schadstoffklasseId) { }
    private record MautkategorieInfo(int kategorieId, double mautsatzJeKm) { }
}
