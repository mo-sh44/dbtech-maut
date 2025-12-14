package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;

import de.htwberlin.dbtech.aufgaben.ue03.TableObjects.Fahrzeug;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FahrzeugMapper {
    private Connection connection;

    public FahrzeugMapper(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection nicht gesetzt");
        }
        return connection;
    }

    /**
     * Liefert das Fahrzeug-Objekt, wenn es aktiv registriert ist,
     * inklusive Achsen und Schadstoffklasse (SSKL_ID).
     */
    public Fahrzeug getVehicleByKennzeichen(String kennzeichen) throws UnkownVehicleException {
        // WICHTIG: Verwenden Sie Großbuchstaben für Tabellen-/Spaltennamen in Oracle
        final String sql = "SELECT FZ_ID, ACHSEN, SSKL_ID FROM FAHRZEUG WHERE KENNZEICHEN = ? AND ABMELDEDATUM IS NULL";

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, kennzeichen);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    Fahrzeug fahrzeug = new Fahrzeug();
                    // ACHTUNG: Hier werden die neuen Setters verwendet
                    fahrzeug.setFzgId(rs.getLong("FZ_ID"));
                    fahrzeug.setAchsen(rs.getInt("ACHSEN"));
                    fahrzeug.setSsklId(rs.getInt("SSKL_ID"));
                    return fahrzeug;
                } else {
                    throw new UnkownVehicleException("Fahrzeug ist nicht aktiv registriert.");
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler beim Abrufen der Fahrzeugdaten: " + e.getMessage(), e);
        }
    }

    /**
     * Liefert ein Fahrzeug-Objekt (oder null), falls ein aktives Fahrzeuggeraet (OBU)
     * fuer das Kennzeichen eingetragen ist.
     */
    public Fahrzeug findFahrzeugMitGeraet(String kennzeichen) {
        final String sql = "SELECT f.FZ_ID FROM FAHRZEUG f " +
                "JOIN FAHRZEUGGERAET fg ON f.FZ_ID = fg.FZ_ID " +
                "WHERE f.KENNZEICHEN = ? AND f.ABMELDEDATUM IS NULL";

        try (PreparedStatement statement = getConnection().prepareStatement(sql)) {
            statement.setString(1, kennzeichen);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    Fahrzeug fahrzeug = new Fahrzeug();
                    fahrzeug.setFzgId(rs.getLong("FZ_ID"));
                    return fahrzeug;
                }
            }
        } catch (SQLException e) {
            throw new DataException("Fehler beim Prüfen des Fahrzeuggeräts: " + e.getMessage(), e);
        }
        return null; // Kein Gerät gefunden
    }
}