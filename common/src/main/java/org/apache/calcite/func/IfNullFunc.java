package org.apache.calcite.func;

import org.apache.commons.lang3.StringUtils;

public class IfNullFunc {

    public Object eval(Object x, Object y) {
        return x == null || StringUtils.isBlank(x.toString()) ? y : x;
    }


}
