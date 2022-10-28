package util;

public class QueryFormatter {
    public String distIdStr(int distId) {
        assert(distId >= 1 && distId <= 10);

        if(distId < 10) {
            return "S_DIST_" + "0" + Integer.toString(distId);
        } else {
            return "S_DIST_" + Integer.toString(distId);
        }
    }
}
