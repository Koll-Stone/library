package bftsmart.tom.core.messages;

public enum XACMLType {
    XACML_nop,
    XACML_UPDATE,
    XACML_QUERY,
    XACML_RE_EXECUTED,
    XACML_RESPONDED;





    public int toInt() {
        switch (this) {
            case XACML_nop: return 0;
            case XACML_UPDATE: return 1;
            case XACML_QUERY: return 2;
            case XACML_RE_EXECUTED: return 3;
            case XACML_RESPONDED: return 4;
            default: return -1;
        }
    }

    public static XACMLType fromInt(int i) {
        switch (i) {
            case 0: return XACML_nop;
            case 1: return XACML_UPDATE;
            case 2: return XACML_QUERY;
            case 3: return XACML_RE_EXECUTED;
            case 4: return XACML_RESPONDED;
            default:
                throw new RuntimeException("type value is "+i+ " Should never reach here!");
        }
    }
}
