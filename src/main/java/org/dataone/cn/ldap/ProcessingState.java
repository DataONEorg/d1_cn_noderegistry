/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.io.Serializable;

/**
 *
 * @author waltz
 */
public enum ProcessingState implements Serializable {

    Offline("offline"), Recovery("recovery"), Active("active");
    private static final long serialVersionUID = 10000000;
    private final String value;

    private ProcessingState(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ProcessingState convert(String value) {
        for (ProcessingState inst : values()) {
            if (inst.getValue().equals(value)) {
                return inst;
            }
        }
        return null;
    }
}
