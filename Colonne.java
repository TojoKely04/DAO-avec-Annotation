package bdd.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Colonne{
    String name() default "";
    boolean isPrimaryKey() default false;
    String pkPrefixe() default "";
    int pkLongueur() default 7;
}
