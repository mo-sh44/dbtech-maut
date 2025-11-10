package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 *
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

    private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
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
    public String getStatusForOnBoardUnit(long fzg_id) {
        String sql = "SELECT status FROM FAHRZEUGGERAT WHERE fzg_id = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fzg_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("status");
                }
            }
        } catch (SQLException e) {
            L.error("Error in getStatusForOnBoardUnit", e);
            throw new DataException(e);
        }
        return null;
    }

    @Override
    public int getUsernumber(int maut_id) {
        String sql = "SELECT nutzer_id FROM MAUTERHEBUNG WHERE maut_id = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, maut_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("nutzer_id");
                }
            }
        } catch (SQLException e) {
            L.error("Error in getUsernumber", e);
            throw new DataException(e);
        }
        return 0;
    }

    @Override
    public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
                                int gewicht, String zulassungsland) {
        String sql = "INSERT INTO FAHRZEUG (fz_id, sskl_id, nutzer_id, kennzeichen, fin, achsen, gewicht, zulassungsland, anmeldedatum) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fz_id);
            ps.setInt(2, sskl_id);
            ps.setInt(3, nutzer_id);
            ps.setString(4, kennzeichen);
            ps.setString(5, fin);
            ps.setInt(6, achsen);
            ps.setInt(7, gewicht);
            ps.setString(8, zulassungsland);
            ps.setDate(9, java.sql.Date.valueOf(LocalDate.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("Error in registerVehicle", e);
            throw new DataException(e);
        }
    }

    @Override
    public void updateStatusForOnBoardUnit(long fzg_id, String status) {
        String sql = "UPDATE FAHRZEUGGERAT SET status = ? WHERE fzg_id = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, fzg_id);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("Error in updateStatusForOnBoardUnit", e);
            throw new DataException(e);
        }
    }

    @Override
    public void deleteVehicle(long fz_id) {
        String sql = "DELETE FROM FAHRZEUG WHERE fz_id = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fz_id);
            ps.executeUpdate();
        } catch (SQLException e) {
            L.error("Error in deleteVehicle", e);
            throw new DataException(e);
        }
    }

    @Override
    public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
        List<Mautabschnitt> result = new ArrayList<>();
        String sql = "SELECT abschnitts_id, laenge, start_koordinate, ziel_koordinate, name, abschnittstyp FROM MAUTABSCHNITT WHERE abschnittstyp = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, abschnittstyp);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Mautabschnitt m = new Mautabschnitt(
                            rs.getInt("abschnitts_id"),
                            rs.getInt("laenge"),
                            rs.getString("start_koordinate"),
                            rs.getString("ziel_koordinate"),
                            rs.getString("name"),
                            rs.getString("abschnittstyp")
                    );
                    result.add(m);
                }
            }
        } catch (SQLException e) {
            L.error("Error in getTrackInformations", e);
            throw new DataException(e);
        }
        return result;
    }
}
