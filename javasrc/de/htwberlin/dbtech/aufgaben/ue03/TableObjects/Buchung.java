package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

import java.sql.Date;

public class Buchung {
    private	int	BUCHUNG_ID;
    private	int	B_ID;
    private	int	ABSCHNITTS_ID;
    private	int	KATEGORIE_ID;
    private	String	KENNZEICHEN;
    private	Date BUCHUNGSDATUM;
    private	Date BEFAHRUNGSDATUM;
    private	Double KOSTEN;

    public Buchung(int BUCHUNG_ID, int b_ID, int ABSCHNITTS_ID, int KATEGORIE_ID, String KENNZEICHEN, Date BUCHUNGSDATUM, Date BEFAHRUNGSDATUM, Double KOSTEN) {
        this.BUCHUNG_ID = BUCHUNG_ID;
        B_ID = b_ID;
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
        this.KATEGORIE_ID = KATEGORIE_ID;
        this.KENNZEICHEN = KENNZEICHEN;
        this.BUCHUNGSDATUM = BUCHUNGSDATUM;
        this.BEFAHRUNGSDATUM = BEFAHRUNGSDATUM;
        this.KOSTEN = KOSTEN;
    }

    public int getBUCHUNG_ID() {
        return BUCHUNG_ID;
    }

    public void setBUCHUNG_ID(int BUCHUNG_ID) {
        this.BUCHUNG_ID = BUCHUNG_ID;
    }

    public int getB_ID() {
        return B_ID;
    }

    public void setB_ID(int b_ID) {
        B_ID = b_ID;
    }

    public int getABSCHNITTS_ID() {
        return ABSCHNITTS_ID;
    }

    public void setABSCHNITTS_ID(int ABSCHNITTS_ID) {
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
    }

    public int getKATEGORIE_ID() {
        return KATEGORIE_ID;
    }

    public void setKATEGORIE_ID(int KATEGORIE_ID) {
        this.KATEGORIE_ID = KATEGORIE_ID;
    }

    public String getKENNZEICHEN() {
        return KENNZEICHEN;
    }

    public void setKENNZEICHEN(String KENNZEICHEN) {
        this.KENNZEICHEN = KENNZEICHEN;
    }

    public Date getBUCHUNGSDATUM() {
        return BUCHUNGSDATUM;
    }

    public void setBUCHUNGSDATUM(Date BUCHUNGSDATUM) {
        this.BUCHUNGSDATUM = BUCHUNGSDATUM;
    }

    public Date getBEFAHRUNGSDATUM() {
        return BEFAHRUNGSDATUM;
    }

    public void setBEFAHRUNGSDATUM(Date BEFAHRUNGSDATUM) {
        this.BEFAHRUNGSDATUM = BEFAHRUNGSDATUM;
    }

    public Double getKOSTEN() {
        return KOSTEN;
    }

    public void setKOSTEN(Double KOSTEN) {
        this.KOSTEN = KOSTEN;
    }
}