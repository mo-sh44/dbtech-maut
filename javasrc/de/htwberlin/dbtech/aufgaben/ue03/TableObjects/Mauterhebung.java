package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

import java.sql.Date;

public class Mauterhebung {
    private int MAUT_ID;
    private int ABSCHNITTS_ID;
    private long FZG_ID;
    private int KATEGORIE_ID;
    private Date BEFAHRUNGSDATUM;
    private double KOSTEN;

    public Mauterhebung (int MAUT_ID, int ABSCHNITTS_ID, long FZG_ID, int KATEGORIE_ID, Date BEFAHRUNGSDATUM, double KOSTEN) {
        this.MAUT_ID = MAUT_ID;
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
        this.FZG_ID = FZG_ID;
        this.KATEGORIE_ID = KATEGORIE_ID;
        this.BEFAHRUNGSDATUM = BEFAHRUNGSDATUM;
        this.KOSTEN = KOSTEN;
    }

    public int getMAUT_ID() {
        return MAUT_ID;
    }

    public void setMAUT_ID(int MAUT_ID) {
        this.MAUT_ID = MAUT_ID;
    }

    public int getABSCHNITTS_ID() {
        return ABSCHNITTS_ID;
    }

    public void setABSCHNITTS_ID(int ABSCHNITTS_ID) {
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
    }

    public long getFZG_ID() {
        return FZG_ID;
    }

    public void setFZG_ID(long FZG_ID) {
        this.FZG_ID = FZG_ID;
    }

    public int getKATEGORIE_ID() {
        return KATEGORIE_ID;
    }

    public void setKATEGORIE_ID(int KATEGORIE_ID) {
        this.KATEGORIE_ID = KATEGORIE_ID;
    }

    public Date getBEFAHRUNGSDATUM() {
        return BEFAHRUNGSDATUM;
    }

    public void setBEFAHRUNGSDATUM(Date BEFAHRUNGSDATUM) {
        this.BEFAHRUNGSDATUM = BEFAHRUNGSDATUM;
    }

    public double getKOSTEN() {
        return KOSTEN;
    }

    public void setKOSTEN(double KOSTEN) {
        this.KOSTEN = KOSTEN;
    }
}