package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

import java.sql.Connection;

/**
 * Die Schnittstelle enthaelt die Services fuer die Mauterhebung.
 *
 * @author Patrick Dohmeier
 */
public interface IMautService {

    /***
     * Die Methode realisiert einen Algorithmus, der die übermittelten
     * Fahrzeugdaten mit der Datenbank auf Richtigkeit überprüft und für einen
     * mautpflichtigen Streckenabschnitt entweder eine Buchung im Manuellen
     * Verfahren schliesst oder die zu zahlende Maut für ein Fahrzeug
     * im Automatischen Verfahren berechnet und eine entsprechende Mauterhebung speichert.
     *
     * Sind die Daten des Fahrzeugs im Automatischen Verfahren korrekt, wird
     * anhand der Mautkategorie (die sich aus der Achszahl und der
     * Schadstoffklasse des Fahrzeugs zusammensetzt) und der Mautabschnittslänge
     * die zu zahlende Maut berechnet und in der Mauterhebung gespeichert.
     *
     * Bei Fahrzeugen im Manuellen Verfahren wird darüberhinaus geprüft,
     * ob es noch offene Buchungen für den Mautabschnitt
     * gibt oder eine Doppelbefahrung aufgetreten ist. Besteht noch eine offene
     * Buchung für den Mautabschnitt, so wird diese Buchung für das Fahrzeug auf
     * abgeschlossen gesetzt.
     *
     * @param mautAbschnitt
     *            - identifiziert einen mautpflichtigen Abschnitt
     * @param achszahl
     *            - identifiziert die Anzahl der Achsen für das Fahrzeug das
     *            durch ein Kontrollsystem erfasst worden ist
     * @param kennzeichen
     *            - idenfiziert das amtliche Kennzeichen des Fahrzeugs das durch
     *            das Kontrollsystem erfasst worden ist
     * @throws UnkownVehicleException
     *             - falls das Fahrzeug weder registriert ist, noch eine offene
     *             Buchung vorliegt
     * @throws InvalidVehicleDataException
     *             - falls Daten des Kontrollsystems nicht mit den hinterlegten
     *             Daten in der Datenbank übereinstimmt
     * @throws AlreadyCruisedException
     *             - falls eine Doppelbefahrung für Fahrzeuge im Manuellen
     *             Verfahren vorliegt
     * @return die berechnete Maut für das Fahrzeug im Automatischen Verfahren
     *         auf dem Streckenabschnitt anhand der Fahrzeugdaten
     */
    void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
            throws UnkownVehicleException, InvalidVehicleDataException,
            AlreadyCruisedException;

    /**
     * Speichert die uebergebene Datenbankverbindung in einer Instanzvariablen.
     *
     * @author Ingo Classen
     */
    void setConnection(Connection connection);

}