package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

import java.sql.Date;

public class Fahrzeug {
    private long FZ_ID;
    private int SSKL_ID;
    private int NUTZER_ID;
    private String KENNZEICHEN;
    private String FIN;
    private int ACHSEN;
    private int GEWICHT;
    private Date ANMELDEDATUM;
    private Date ABMELDEDATUM;
    private String ZULASSUNGSLAND;

    // Standardkonstruktor für Mappers
    public Fahrzeug() {
    }

    // Konstruktor
    public Fahrzeug(long FZ_ID, int SSKL_ID, int NUTZER_ID, String KENNZEICHEN, String FIN, int ACHSEN, int GEWICHT, Date ANMELDEDATUM, Date ABMELDEDATUM, String ZULASSUNGSLAND) {
        this.FZ_ID = FZ_ID;
        this.SSKL_ID = SSKL_ID;
        this.NUTZER_ID = NUTZER_ID;
        this.KENNZEICHEN = KENNZEICHEN;
        this.FIN = FIN;
        this.ACHSEN = ACHSEN;
        this.GEWICHT = GEWICHT;
        this.ANMELDEDATUM = ANMELDEDATUM;
        this.ABMELDEDATUM = ABMELDEDATUM;
        this.ZULASSUNGSLAND = ZULASSUNGSLAND;
    }

    // =======================================================
    // GETTERS (Korrigierte CamelCase-Versionen)
    // =======================================================

    // FZ_ID
    public long getFzgId() {
        return FZ_ID;
    }

    // ACHSEN
    public int getAchsen() {
        return ACHSEN;
    }

    // SSKL_ID
    public int getSsklId() {
        return SSKL_ID;
    }


    public long getFZ_ID() {
        return FZ_ID;
    }

    public int getACHSEN() {
        return ACHSEN;
    }

    public int getSSKL_ID() {
        return SSKL_ID;
    }


    // =======================================================
    // SETTERS (Korrigierte CamelCase-Versionen)
    // =======================================================

    // FZ_ID
    public void setFzgId(long FZ_ID) {
        this.FZ_ID = FZ_ID;
    }

    // ACHSEN
    public void setAchsen(int ACHSEN) {
        this.ACHSEN = ACHSEN;
    }

    // SSKL_ID
    public void setSsklId(int SSKL_ID) {
        this.SSKL_ID = SSKL_ID;
    }


    public void setFZ_ID(long FZ_ID) {
        this.FZ_ID = FZ_ID;
    }

    public void setACHSEN(int ACHSEN) {
        this.ACHSEN = ACHSEN;
    }

    public void setSSKL_ID(int SSKL_ID) {
        this.SSKL_ID = SSKL_ID;
    }


    public int getNUTZER_ID() {
        return NUTZER_ID;
    }

    public void setNUTZER_ID(int NUTZER_ID) {
        this.NUTZER_ID = NUTZER_ID;
    }

    public String getKENNZEICHEN() {
        return KENNZEICHEN;
    }

    public void setKENNZEICHEN(String KENNZEICHEN) {
        this.KENNZEICHEN = KENNZEICHEN;
    }

    public String getFIN() {
        return FIN;
    }

    public void setFIN(String FIN) {
        this.FIN = FIN;
    }

    public int getGEWICHT() {
        return GEWICHT;
    }

    public void setGEWICHT(int GEWICHT) {
        this.GEWICHT = GEWICHT;
    }

    public Date getANMELDEDATUM() {
        return ANMELDEDATUM;
    }

    public void setANMELDEDATUM(Date ANMELDEDATUM) {
        this.ANMELDEDATUM = ANMELDEDATUM;
    }

    public Date getABMELDEDATUM() {
        return ABMELDEDATUM;
    }

    public void setABMELDEDATUM(Date ABMELDEDATUM) {
        this.ABMELDEDATUM = ABMELDEDATUM;
    }

    public String getZULASSUNGSLAND() {
        return ZULASSUNGSLAND;
    }

    public void setZULASSUNGSLAND(String ZULASSUNGSLAND) {
        this.ZULASSUNGSLAND = ZULASSUNGSLAND;
    }
}