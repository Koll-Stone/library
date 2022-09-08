package bftsmart.tom.core.messages;

public enum XACMLType {
    XACML_nop,
    XACML_UPDATE,
    XACML_QUERY;



    public int toInt() {
        switch (this) {
            case XACML_nop: return 0;
            case XACML_UPDATE: return 1;
            case XACML_QUERY: return 2;
            default: return -1;
        }
    }

    public static XACMLType fromInt(int i) {
        switch (i) {
            case 0: return XACML_nop;
            case 1: return XACML_UPDATE;
            case 2: return XACML_QUERY;
            default:
                throw new RuntimeException("Should never reach here!");
        }
    }
}
