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

    public void setFZ_ID(long FZ_ID) {
        this.FZ_ID = FZ_ID;
    }

    public void setSSKL_ID(int SSKL_ID) {
        this.SSKL_ID = SSKL_ID;
    }

    public void setNUTZER_ID(int NUTZER_ID) {
        this.NUTZER_ID = NUTZER_ID;
    }

    public void setKENNZEICHEN(String KENNZEICHEN) {
        this.KENNZEICHEN = KENNZEICHEN;
    }

    public void setFIN(String FIN) {
        this.FIN = FIN;
    }

    public void setACHSEN(int ACHSEN) {
        this.ACHSEN = ACHSEN;
    }

    public void setGEWICHT(int GEWICHT) {
        this.GEWICHT = GEWICHT;
    }

    public void setANMELDEDATUM(Date ANMELDEDATUM) {
        this.ANMELDEDATUM = ANMELDEDATUM;
    }

    public void setABMELDEDATUM(Date ABMELDEDATUM) {
        this.ABMELDEDATUM = ABMELDEDATUM;
    }

    public void setZULASSUNGSLAND(String ZULASSUNGSLAND) {
        this.ZULASSUNGSLAND = ZULASSUNGSLAND;
    }

    public long getFZ_ID() {
        return FZ_ID;
    }

    public int getSSKL_ID() {
        return SSKL_ID;
    }

    public int getNUTZER_ID() {
        return NUTZER_ID;
    }

    public String getKENNZEICHEN() {
        return KENNZEICHEN;
    }

    public String getFIN() {
        return FIN;
    }

    public int getACHSEN() {
        return ACHSEN;
    }

    public int getGEWICHT() {
        return GEWICHT;
    }

    public Date getANMELDEDATUM() {
        return ANMELDEDATUM;
    }

    public Date getABMELDEDATUM() {
        return ABMELDEDATUM;
    }

    public String getZULASSUNGSLAND() {
        return ZULASSUNGSLAND;
    }
}