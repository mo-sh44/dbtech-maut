package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MauterhebungMapper {
    private static final Logger L = LoggerFactory.getLogger(MauterhebungMapper.class);
    private Connection connection;

    public MauterhebungMapper(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection nicht gesetzt");
        }
        return connection;
    }

    /**
     * Prüft, ob das Fahrzeug für diesen Abschnitt bereits eine Mauterhebung hat (Doppelbefahrung im automatischen Verfahren).
     */
    public boolean isAlreadyRecorded(long fzgId, int abschnittsId) {
        final String sql = "SELECT 1 FROM MAUTERHEBUNG WHERE FZG_ID = ? AND ABSCHNITTS_ID = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fzgId);
            ps.setInt(2, abschnittsId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new DataException("Fehler bei der Doppelbefahrungsprüfung (Mauterhebung).", e);
        }
    }

    /**
     * Berechnet die Maut und speichert einen neuen Eintrag in MAUTERHEBUNG (Automatisches Verfahren).
     */
    public void berechneUndSpeichereMaut(long fzgId, int ssklId, int mautAbschnitt, int achsen) {

        try {
            // 1. Hole Mautsatz und Kategorie-ID
            double mautJeKm;
            int katId;

            // JOIN FAHRZEUG/MAUTKATEGORIE/MAUTSATZ, um den Satz (KOSTEN) zu finden
            String sqlMaut =
                    "SELECT mk.KATEGORIE_ID, ms.KOSTEN FROM MAUTSATZ ms " +
                            "JOIN MAUTKATEGORIE mk ON ms.KATEGORIE_ID = mk.KATEGORIE_ID " +
                            "WHERE ms.ABSCHNITTS_ID = ? AND mk.SSKL_ID = ?"; // Vereinfacht, da Achs-Check bereits im Service stattfand

            try (PreparedStatement ps = getConnection().prepareStatement(sqlMaut)) {
                ps.setInt(1, mautAbschnitt);
                ps.setInt(2, ssklId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        mautJeKm = rs.getDouble("KOSTEN");
                        katId = rs.getInt("KATEGORIE_ID");
                    } else {
                        throw new DataException("Mautsatz/Kategorie für Abschnitt " + mautAbschnitt + " und SSKL " + ssklId + " nicht gefunden.");
                    }
                }
            }

            // 2. Hole Abschnittslänge
            double laenge;
            String sqlLaenge = "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?";
            try (PreparedStatement ps = getConnection().prepareStatement(sqlLaenge)) {
                ps.setInt(1, mautAbschnitt);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        laenge = rs.getDouble(1);
                    } else {
                        throw new DataException("Abschnittslänge nicht gefunden.");
                    }
                }
            }

            // 3. Berechnung und Rundung (laenge / 1000, falls laenge in Metern und Maut in Km)
            double roheKosten = (laenge / 1000.0) * mautJeKm;

            // WICHTIG: Rundung auf 2 Dezimalstellen (für Test 6)
            BigDecimal kosten = BigDecimal.valueOf(roheKosten).setScale(2, RoundingMode.HALF_UP);

            // 4. MAUTERHEBUNG einfügen (Verwendung von MAUT_ID_SEQ.NEXTVAL)
            String sqlInsert =
                    "INSERT INTO MAUTERHEBUNG (MAUT_ID, FZ_ID, ABSCHNITTS_ID, KATEGORIE_ID, KOSTEN, BEFAHRUNGSDATUM) " +
                            "VALUES (MAUT_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?)";

            try (PreparedStatement ps = getConnection().prepareStatement(sqlInsert)) {
                ps.setLong(1, fzgId); // FZ_ID
                ps.setInt(2, mautAbschnitt);
                ps.setInt(3, katId);
                ps.setBigDecimal(4, kosten); // KOSTEN
                ps.setDate(5, Date.valueOf(LocalDate.now()));
                ps.executeUpdate();
                L.info("Mauterhebung für FZ {} auf Abschnitt {} gespeichert. Kosten: {}", fzgId, mautAbschnitt, kosten);
            }

        } catch (SQLException e) {
            throw new DataException("Fehler beim automatischen Verfahren in MauterhebungMapper.", e);
        }
    }
}