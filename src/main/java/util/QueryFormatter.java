package util;

public class QueryFormatter {
    public String distIdStr(int distId) {
        assert(distId >= 1 && distId <= 10);

        if(distId < 10) {
            return "0" + Integer.toString(distId);
        } else {
            return Integer.toString(distId);
        }
    }
}
