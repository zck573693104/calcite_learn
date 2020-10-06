package org.apache.calcite.func;


import java.util.UUID;

public class UUIDFunc {

    public String eval() {

        return UUID.randomUUID().toString();
    }
}
