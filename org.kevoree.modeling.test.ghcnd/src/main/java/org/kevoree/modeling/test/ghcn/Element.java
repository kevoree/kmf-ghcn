package org.kevoree.modeling.test.ghcn;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by gregory.nain on 01/08/2014.
 */
public abstract class Element {
    public static class ElementType {
        public String key;
        public String name;
        public String description;
        public float precision;
        public String unit;

        public ElementType(String key, String name, String description, float precision, String unit) {
            this.key = key;
            this.name = name;
            this.description = description;
            this.precision = precision;
            this.unit = unit;
        }
    }


    public static final ElementType PRCP = new ElementType("PRCP", "Rain", "Precipitation (tenths of mm)", 0.1f, "mm");
    public static final ElementType SNOW = new ElementType("SNOW", "Snow Fall", "Snowfall (mm)", 1f, "mm");
    public static final ElementType SNWD = new ElementType("SNWD", "Snow Depth", "Snow depth (mm)", 1f, "mm");
    public static final ElementType TMAX = new ElementType("TMAX", "T°Max", "Maximum temperature (tenths of degrees C)", 0.1f, "°C");
    public static final ElementType TMIN = new ElementType("TMIN", "T°Min", "Minimum temperature (tenths of degrees C)", 0.1f, "°C");
    public static final ElementType ACMC = new ElementType("ACMC", "Cloudiness M2M(Ceilometer)", "Average cloudiness midnight to midnight from 30-second ceilometer data (percent)", 1f, "%");
    public static final ElementType ACMH = new ElementType("ACMH", "Cloudiness M2M(Manual)", "Average cloudiness midnight to midnight from manual observations (percent)", 1f, "%");
    public static final ElementType ACSC = new ElementType("ACMC", "Cloudiness S2S(Ceilometer)", "Average cloudiness sunrise to sunset from 30-second ceilometer data (percent)", 1f, "%");
    public static final ElementType ACSH = new ElementType("ACSH", "Cloudiness S2S(Manual)", "Average cloudiness sunrise to sunset from manual observations (percent)", 1f, "%");
    public static final ElementType AWND = new ElementType("AWND", "Avg. Day Wind Speed", "Average daily wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType DAEV = new ElementType("DAEV", "Days in MDEV", "Number of days included in the multiday evaporation total (MDEV)", 1f, "days");
    public static final ElementType DAPR = new ElementType("DAPR", "Days in MDPR", "Number of days included in the multiday precipiation total (MDPR)", 1f, "days");
    public static final ElementType DASF = new ElementType("DASF", "Days in MDSF", "Number of days included in the multiday snowfall total (MDSF)", 1f, "days");
    public static final ElementType DATN = new ElementType("DATN", "Days in MDTN", "Number of days included in the multiday minimum temperature (MDTN)", 1f, "days");
    public static final ElementType DATX = new ElementType("DATX", "Days in MDTX", "Number of days included in the multiday maximum temperature (MDTX)", 1f, "days");
    public static final ElementType DAWM = new ElementType("DAWM", "Days in MDWM", "Number of days included in the multiday wind movement (MDWM)", 1f, "days");
    public static final ElementType DWPR = new ElementType("DWPR", "Days in MDPR", "Number of days with non-zero precipitation included in multiday precipitation total (MDPR)", 1f, "days");
    public static final ElementType EVAP = new ElementType("EVAP", "Evaporation", "Evaporation of water from evaporation pan (tenths of mm)", 0.1f, "mm");
    public static final ElementType FMTM = new ElementType("FMTM", "Fastest Wind Time (1min)", "Time of fastest mile or fastest 1-minute wind (hours and minutes, i.e., HHMM)", 1f, "HH:mm");
    public static final ElementType FRGB = new ElementType("FRGB", "Frozen Ground Base", "Base of frozen ground layer (cm)", 1f, "cm");
    public static final ElementType FRGT = new ElementType("FRGT", "Frozen Ground Top", "Top of frozen ground layer (cm)", 1f, "cm");
    public static final ElementType FRTH = new ElementType("FRTH", "Frozen Ground Thickness", "Thickness of frozen ground layer (cm)", 1f, "cm");
    public static final ElementType GAHT = new ElementType("GAHT", "Gauge Level", "Difference between river and gauge height (cm)", 1f, "cm");
    public static final ElementType MDEV = new ElementType("MDEV", "Total Evaporation (Multiday)", "Multiday evaporation total (tenths of mm; use with DAEV)", 0.1f, "mm");
    public static final ElementType MDPR = new ElementType("MDPR", "Total Precipitations (Multiday)", "Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)", 0.1f, "mm");
    public static final ElementType MDSF = new ElementType("MDSF", "Total Snawfall (Multiday)", "Multiday snowfall total", 0.1f, "mm");
    public static final ElementType MDTN = new ElementType("MDTN", "Minimum Temperature (Multiday)", "Multiday minimum temperature (tenths of degrees C; use with DATN)", 0.1f, "°C");
    public static final ElementType MDTX = new ElementType("MDTX", "Maximum Temperature (Multiday)", "Multiday maximum temperature (tenths of degress C; use with DATX)", 0.1f, "°C");
    public static final ElementType MDWM = new ElementType("MDWM", "Wind Movement (Multiday)", "Multiday wind movement (km)", 1f, "km");
    public static final ElementType MNPN = new ElementType("MNPN", "Minimum Daily Temp", "Daily minimum temperature of water in an evaporation pan (tenths of degrees C)", 0.1f, "°C");
    public static final ElementType MXPN = new ElementType("MXPN", "Maximum Daily Temp", "Daily maximum temperature of water in an evaporation pan (tenths of degrees C)", 0.1f, "°C");
    public static final ElementType PGTM = new ElementType("PGTM", "Peak Gust Time", "Peak gust time (hours and minutes, i.e., HHMM)", 1f, "HH:mm");
    public static final ElementType PSUN = new ElementType("PSUN", "Possible Daily Sunshine", "Daily percent of possible sunshine (percent)", 1f, "%");
    public static final ElementType THIC = new ElementType("THIC", "Ice Thickness (Watter)", "Thickness of ice on water (tenths of mm)", 0.1f, "mm");
    public static final ElementType TOBS = new ElementType("TOBS", "Observation Time Temp.", "Temperature at the time of observation (tenths of degrees C)", 0.1f, "°C");
    public static final ElementType TSUN = new ElementType("TSUN", "Daily Total Sunshine", "Daily total sunshine (minutes)", 1f, "min");
    public static final ElementType WDF1 = new ElementType("WDF1", "Fastest Wind Direction (1m)", "Direction of fastest 1-minute wind (degrees)", 1f, "°");
    public static final ElementType WDF2 = new ElementType("WDF2", "Fastest Wind Direction (2min)", "Direction of fastest 2-minute wind (degrees)", 1f, "°");
    public static final ElementType WDF5 = new ElementType("WDF5", "Fastest Wind Direction (5min)", "Direction of fastest 5-minute wind (degrees)", 1f, "°");
    public static final ElementType WDFG = new ElementType("WDFG", "Peak Wind Gust Direction", "Direction of peak wind gust (degrees)", 1f, "°");
    public static final ElementType WDFI = new ElementType("WDFI", "Highest Instantaneous Wind Direction", "Direction of highest instantaneous wind (degrees)", 1f, "°");
    public static final ElementType WDFM = new ElementType("WDFM", "Fastest Mile Direction (1m)", "Fastest mile wind direction (degrees)", 1f, "°");
    public static final ElementType WDMV = new ElementType("WDMV", "Wind Movement(24h)", "24-hour wind movement (km)", 1f, "km");
    public static final ElementType WESD = new ElementType("WESD", "Watter Equivalent of Snow", "Water equivalent of snow on the ground (tenths of mm)", 0.1f, "mm");
    public static final ElementType WESF = new ElementType("WESF", "Watter Equivalent of Snowfall", "Water equivalent of snowfall (tenths of mm)", 0.1f, "mm");
    public static final ElementType WSF1 = new ElementType("WSF1", "Fastest Wind Speed (1min)", "Fastest 1-minute wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType WSF2 = new ElementType("WSF2", "Fastest Wind Speed (2min)", "Fastest 2-minute wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType WSF5 = new ElementType("WSF5", "Fastest Wind Speed (5min)", "Fastest 5-minute wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType WSFG = new ElementType("WSFG", "Peak Gust Wind Speed", "Peak guest wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType WSFI = new ElementType("WSFI", "Highest Instantaneous Wind Speed", "Highest instantaneous wind speed (tenths of meters per second)", 0.1f, "m/s");
    public static final ElementType WSFM = new ElementType("WDMV", "Fastest Mile Wind Speed", "Fastest mile wind speed (tenths of meters per second)", 0.1f, "m/s");
    private static final HashMap<String, ElementType> elementTypes = new HashMap<String, ElementType>(15);

    static {
        elementTypes.put(PRCP.key, PRCP);
        elementTypes.put(SNOW.key, SNOW);
        elementTypes.put(SNWD.key, SNWD);
        elementTypes.put(TMAX.key, TMAX);
        elementTypes.put(TMIN.key, TMIN);
        elementTypes.put(ACMC.key, ACMC);
        elementTypes.put(ACMH.key, ACMH);
        elementTypes.put(ACSC.key, ACSC);
        elementTypes.put(ACSH.key, ACSH);
        elementTypes.put(AWND.key, AWND);
        elementTypes.put(DAEV.key, DAEV);
        elementTypes.put(DAPR.key, DAPR);
        elementTypes.put(DASF.key, DASF);
        elementTypes.put(DATN.key, DATN);
        elementTypes.put(DATX.key, DATX);
        elementTypes.put(DAWM.key, DAWM);
        elementTypes.put(DWPR.key, DWPR);
        elementTypes.put(EVAP.key, EVAP);
        elementTypes.put(FMTM.key, FMTM);
        elementTypes.put(FRGB.key, FRGB);
        elementTypes.put(FRGT.key, FRGT);
        elementTypes.put(FRTH.key, FRTH);
        elementTypes.put(GAHT.key, GAHT);
        elementTypes.put(MDEV.key, MDEV);
        elementTypes.put(MDPR.key, MDPR);
        elementTypes.put(MDSF.key, MDSF);
        elementTypes.put(MDTN.key, MDTN);
        elementTypes.put(MDTX.key, MDTX);
        elementTypes.put(MDWM.key, MDWM);
        elementTypes.put(MNPN.key, MNPN);
        elementTypes.put(MXPN.key, MXPN);
        elementTypes.put(PGTM.key, PGTM);
        elementTypes.put(PSUN.key, PSUN);
        elementTypes.put(THIC.key, THIC);
        elementTypes.put(TOBS.key, TOBS);
        elementTypes.put(TSUN.key, TSUN);
        elementTypes.put(WDF1.key, WDF1);
        elementTypes.put(WDF2.key, WDF2);
        elementTypes.put(WDF5.key, WDF5);
        elementTypes.put(WDFG.key, WDFG);
        elementTypes.put(WDFI.key, WDFI);
        elementTypes.put(WDFM.key, WDFM);
        elementTypes.put(WDMV.key, WDMV);
        elementTypes.put(WESD.key, WESD);
        elementTypes.put(WESF.key, WESF);
        elementTypes.put(WSF1.key, WSF1);
        elementTypes.put(WSF2.key, WSF2);
        elementTypes.put(WSF5.key, WSF5);
        elementTypes.put(WSFG.key, WSFG);
        elementTypes.put(WSFI.key, WSFI);
        elementTypes.put(WSFM.key, WSFM);




    }

    public static Collection<ElementType> getElementTypeList() {
        return elementTypes.values();
    }

    public static ElementType parse(String s) {
        ElementType tmp = elementTypes.get(s);
        if(tmp != null){return tmp;}
        if(s.startsWith("SN") || s.startsWith("SX")){return buildSoilTemp(s);}
        if(s.startsWith("WT") ){return buildWeatherType(s);}
        if(s.startsWith("WV") ){return buildWeatherVicinity(s);}
        return null;
    }



    private static ElementType buildSoilTemp(String s){
        if(!s.startsWith("SN") && !s.startsWith("SX")){return null;}
        int groundCode = Integer.valueOf(""+s.charAt(2));
        int soilCode = Integer.valueOf(""+s.charAt(3));
        ElementType tmp = new ElementType("SN" + groundCode + "" + soilCode, (s.charAt(1)=='N'?"Minimum":"Maximum")+" soil temperature", (s.charAt(1)=='N'?"Minimum":"Maximum")+" soil temperature ("+getSoilDepth(soilCode)+" of "+getGroundCover(groundCode)+")(tenths of degrees C)", 0.1f, "°C");
        elementTypes.put(tmp.key, tmp);
        return tmp;
    }

    private static String getGroundCover(int s) {
        switch(s) {
            case 1:return"Grass";
            case 2:return"Fallow";
            case 3:return"Bare Ground";
            case 4:return"Brome Grass";
            case 5:return"Sod";
            case 6:return"Straw multch";
            case 7:return"Grass Muck";
            case 8:return"Bare Muck";
            case 0:
            default:return"Unknown";
        }
    }

    private static String getSoilDepth(int s) {
        switch(s) {
            case 1:return"5 cm";
            case 2:return"10 cm";
            case 3:return"20 cm";
            case 4:return"50 cm";
            case 5:return"100 cm";
            case 6:return"150 cm";
            case 7:return"180 cm";
            default:return"Unknown";
        }
    }

    private static ElementType buildWeatherType(String s){
        if(!s.startsWith("WT")){return null;}
        int type = Integer.valueOf(""+s.charAt(2)+s.charAt(3));
        ElementType tmp = new ElementType("WT" + s.charAt(2)+s.charAt(3), getWeatherType(type), "Weather Type: " + getWeatherType(type), 1f, "");
        elementTypes.put(tmp.key, tmp);
        return tmp;
    }

    private static String getWeatherType(int s) {
        switch(s) {
            case 1:return "Fog, ice fog, or freezing fog (may include heavy fog)";
            case 2:return "Heavy fog or heaving freezing fog (not always distinquished from fog)";
            case 3:return "Thunder";
            case 4:return "Ice pellets, sleet, snow pellets, or small hail";
            case 5:return "Hail (may include small hail)";
            case 6:return "Glaze or rime";
            case 7:return "Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction";
            case 8:return "Smoke or haze";
            case 9:return "Blowing or drifting snow";
            case 10:return "Tornado, waterspout, or funnel cloud";
            case 11:return "High or damaging winds";
            case 12:return "Blowing spray";
            case 13:return "Mist";
            case 14:return "Drizzle";
            case 15:return "Freezing drizzle";
            case 16:return "Rain (may include freezing rain, drizzle, and freezing drizzle)";
            case 17:return "Freezing rain";
            case 18:return "Snow, snow pellets, snow grains, or ice crystals";
            case 19:return "Unknown source of precipitation";
            case 21:return "Ground fog";
            case 22:return "Ice fog or freezing fog";
            default: return "Unknown";
        }
    }


    private static ElementType buildWeatherVicinity(String s){
        if(!s.startsWith("WV")){return null;}
        int type = Integer.valueOf(""+s.charAt(2)+s.charAt(3));
        ElementType tmp = new ElementType("WV" + s.charAt(2)+s.charAt(3), getWeatherVicinity(type), "Weather Vicinity: " + getWeatherVicinity(type), 1f, "");
        elementTypes.put(tmp.key, tmp);
        return tmp;
    }

    private static String getWeatherVicinity(int s) {
        switch(s) {
            case 1:return "Fog, ice fog, or freezing fog (may include heavy fog)";
            case 3:return "Thunder";
            case 7:return "Ash, dust, sand, or other blowing obstruction";
            case 18:return "Snow or ice crystals";
            case 20:return "Rain or snow shower";
            default: return "Unknown";
        }
    }

}

