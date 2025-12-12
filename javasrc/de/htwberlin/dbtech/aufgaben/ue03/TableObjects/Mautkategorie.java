package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

public class Mautkategorie {
    private int KATEGORIE_ID;
    private int SSKL_ID;
    private String KAT_BEZEICHNUNG;
    private int ACHSZAHL;
    private double MAUTSATZ_JE_KM;

    public Mautkategorie(int KATEGORIE_ID, int SSKL_ID, String KAT_BEZEICHNUNG, int ACHSZAHL, double MAUTSATZ_JE_KM) {
        this.KATEGORIE_ID = KATEGORIE_ID;
        this.SSKL_ID = SSKL_ID;
        this.KAT_BEZEICHNUNG = KAT_BEZEICHNUNG;
        this.ACHSZAHL = ACHSZAHL;
        this.MAUTSATZ_JE_KM = MAUTSATZ_JE_KM;
    }

    public int getKATEGORIE_ID() {
        return KATEGORIE_ID;
    }

    public void setKATEGORIE_ID(int KATEGORIE_ID) {
        this.KATEGORIE_ID = KATEGORIE_ID;
    }

    public int getSSKL_ID() {
        return SSKL_ID;
    }

    public void setSSKL_ID(int SSKL_ID) {
        this.SSKL_ID = SSKL_ID;
    }

    public String getKAT_BEZEICHNUNG() {
        return KAT_BEZEICHNUNG;
    }

    public void setKAT_BEZEICHNUNG(String KAT_BEZEICHNUNG) {
        this.KAT_BEZEICHNUNG = KAT_BEZEICHNUNG;
    }

    public int getACHSZAHL() {
        return ACHSZAHL;
    }

    public void setACHSZAHL(int ACHSZAHL) {
        this.ACHSZAHL = ACHSZAHL;
    }

    public double getMAUTSATZ_JE_KM() {
        return MAUTSATZ_JE_KM;
    }

    public void setMAUTSATZ_JE_KM(double MAUTSATZ_JE_KM) {
        this.MAUTSATZ_JE_KM = MAUTSATZ_JE_KM;
    }
}