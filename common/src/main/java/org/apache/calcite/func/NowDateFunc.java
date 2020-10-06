package org.apache.calcite.func;

import java.time.LocalDateTime;

public class NowDateFunc {

    public LocalDateTime eval(){
        return LocalDateTime.now();
    }
}
