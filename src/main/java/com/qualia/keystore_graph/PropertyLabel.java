package com.qualia.keystore_graph;


public enum PropertyLabel {
    TYPE(PropertyType.STRING),
    PLATFORM(PropertyType.STRING),
    BROWSER(PropertyType.STRING), 
    LAST_SEEN(PropertyType.INTEGER);

    private final PropertyType propertyType;


    PropertyLabel(PropertyType propertyType) {
        this.propertyType = propertyType;
    }


    public PropertyType getPropertyType() {
        return propertyType;
    }
}
