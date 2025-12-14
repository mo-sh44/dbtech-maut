package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.aufgaben.ue03.DaoMapper.BuchungMapper;
import de.htwberlin.dbtech.aufgaben.ue03.DaoMapper.FahrzeugMapper;
import de.htwberlin.dbtech.aufgaben.ue03.DaoMapper.MauterhebungMapper;
import de.htwberlin.dbtech.aufgaben.ue03.TableObjects.Fahrzeug;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Implementierung des Maut-Services (Uebung 3, Variante mit DAO/Mapper-Pattern).
 */
public class MautServiceImplDao implements IMautService {

    private static final Logger L = LoggerFactory.getLogger(MautServiceImplDao.class);

    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection nicht gesetzt");
        }
        return connection;
    }

    @Override
    public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
            throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {

        Connection conn = getConnection();

        // Mapper initialisieren
        FahrzeugMapper fahrzeugMapper = new FahrzeugMapper(conn);
        BuchungMapper buchungMapper = new BuchungMapper(conn);
        MauterhebungMapper mauterhebungMapper = new MauterhebungMapper(conn);

        Fahrzeug registriertesFahrzeug = null;
        boolean istFahrzeugGefunden = true;

        // 1) FAHRZEUG PRÜFEN: Versuch, Fahrzeugdaten abzurufen
        try {
            registriertesFahrzeug = fahrzeugMapper.getVehicleByKennzeichen(kennzeichen);
        } catch (UnkownVehicleException e) {
            istFahrzeugGefunden = false;
        }

        // 2) BUCHUNG PRÜFEN & UNKNOWN VEHICLE CHECK
        Integer offeneBuchungId = buchungMapper.findOffeneBuchungId(kennzeichen);

        if (!istFahrzeugGefunden && offeneBuchungId == null) {
            // Weder Registrierung noch offene Buchung vorhanden
            throw new UnkownVehicleException();
        }

        // 3) ACHSZAHL VALIDIEREN (DATEN PRÜFEN)
        if (istFahrzeugGefunden) {
            // A) Automatisch/Registriert: Vergleich mit Achsen aus FAHRZEUG
            // KORRIGIERT: verwendet registriertesFahrzeug.getAchsen()
            if (registriertesFahrzeug.getAchsen() != achszahl) {
                throw new InvalidVehicleDataException();
            }
        } else {
            // B) Manuell (Offene Buchung): Vergleich mit Achsen aus MAUTKATEGORIE der Buchung
            // KORRIGIERT: verwendet die korrigierte Funktion in BuchungMapper
            int buchungAchsen = buchungMapper.getAchsenFuerOffeneBuchung(kennzeichen);
            if (buchungAchsen != achszahl) {
                throw new InvalidVehicleDataException();
            }
        }

        // 4) VERFAHREN UNTERSCHEIDEN
        Fahrzeug fahrzeugMitGeraet = fahrzeugMapper.findFahrzeugMitGeraet(kennzeichen);

        if (fahrzeugMitGeraet != null && istFahrzeugGefunden) {
            // ===================== AUTOMATISCHES VERFAHREN =====================

            // 4.1 Doppelbefahrung prüfen
            // KORRIGIERT: verwendet registriertesFahrzeug.getFzgId()
            if (mauterhebungMapper.isAlreadyRecorded(registriertesFahrzeug.getFzgId(), mautAbschnitt)) {
                throw new AlreadyCruisedException();
            }

            // 4.2 Maut berechnen und speichern
            mauterhebungMapper.berechneUndSpeichereMaut(
                    registriertesFahrzeug.getFzgId(), // KORRIGIERT: verwendet getFzgId()
                    registriertesFahrzeug.getSsklId(), // KORRIGIERT: verwendet getSsklId()
                    mautAbschnitt,
                    achszahl
            );
            L.info("Automatische Maut fuer Fahrzeug {} auf Abschnitt {} verbucht", kennzeichen, mautAbschnitt);

        } else {
            // ===================== MANUELLES VERFAHREN =====================

            // 4.3 Doppelbefahrung prüfen (ob bereits abgeschlossen)
            if (buchungMapper.istDoppelbefahrungAbgeschlossen(mautAbschnitt, kennzeichen)) {
                throw new AlreadyCruisedException();
            }

            // 4.4 Buchung abschliessen
            if (offeneBuchungId != null) {
                buchungMapper.markiereBuchungAlsAbgeschlossen(offeneBuchungId);
                L.info("Buchung {} fuer Fahrzeug {} wurde auf 'abgeschlossen' gesetzt", offeneBuchungId, kennzeichen);
            } else {
                // Falls Fzg. registriert, aber keine OBU und keine offene Buchung -> AlreadyCruised (wie im Freundcode)
                throw new AlreadyCruisedException();
            }
        }
    }
}