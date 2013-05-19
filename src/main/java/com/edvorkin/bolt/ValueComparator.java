package com.edvorkin.bolt;

import java.util.Comparator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/4/13
 * Time: 2:34 PM
 * To change this template use File | Settings | File Templates.
 */

    class ValueComparator implements Comparator<String> {

        Map<String, Long> base;
        public ValueComparator(Map<String, Long> base) {
            this.base = base;
        }

        // Note: this comparator imposes orderings that are inconsistent with equals.
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }

