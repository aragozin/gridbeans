package org.gridkit.lab.gridbeans.monadic;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LocatorShortcut {

    public Class<? extends Locator> locatorType();
    
    public String method();
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public @interface LocationId {
        
    }    
}
