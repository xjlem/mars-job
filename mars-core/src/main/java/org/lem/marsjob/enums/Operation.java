package org.lem.marsjob.enums;

/**
 * @author Administrator
 */

public enum Operation {
    UPDATE_CRON(0), ADD(1), REMOVE(2);
    private int operationCode;

    Operation(int operationCode) {
        this.operationCode = operationCode;
    }

    public static Operation getOperationByCode(int code) {
        Operation[] values = values();
        for (Operation en : values) {
            if (en.getOperationCode() == code) {
                return en;
            }
        }
        return null;
    }

    public int getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    @Override
    public String toString() {
        return this.name();
    }
}
