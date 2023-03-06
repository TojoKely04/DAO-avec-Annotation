package bdd;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Vector;
import bdd.annotation.Table;
import bdd.annotation.Colonne;

public class BddObject {

    public Vector select(Connection connection) throws Exception
    {
        String table = this.tableNameAnnotation();
        boolean createConnection=false;
        if(connection==null){
            createConnection = true;
            connection = Connect.getConnection();
        }
        Statement stat=connection.createStatement();
        Vector listObjet=new Vector();
        Vector<Field> listField=this.getFields();

        String sql ="SELECT * FROM "+table.toUpperCase();
        ResultSet resultSet=stat.executeQuery(sql);
        while (resultSet.next()) 
        {
            Object objet=this.resultSetToObject(listField,resultSet);
            listObjet.add(objet);
        }
        stat.close(); 

        System.out.println(sql);
        if(createConnection == true ) connection.close();
        return listObjet;
    }       

    public Object selectById(Connection connection, Object id) throws Exception
    {
        String table = this.tableNameAnnotation();
        boolean createConnection=false;
        if(connection==null){
            createConnection = true;
            connection = Connect.getConnection();
        }
        String primaryKey = BddObject.getPrimaryKeyName(connection,table);
        Statement stat=connection.createStatement();
        Vector listObjet=new Vector();
        Vector<Field> listField=this.getFields();

        String sql = "SELECT * FROM "+table.toUpperCase()+" WHERE "+primaryKey+" = '"+String.valueOf(id)+"'";
        ResultSet resultSet=stat.executeQuery(sql);
        resultSet.next();
        Object objet=this.resultSetToObject(listField,resultSet);
        
        System.out.println(sql);
        stat.close(); 

        if(createConnection == true ) connection.close();
        return objet;
    }    

    public void insert(Connection connection) throws Exception
    {
        String table = this.tableNameAnnotation();
        boolean createConnection=false;
        if(connection==null){
            createConnection = true;
            connection = Connect.getConnection();
        }

        Statement statement=connection.createStatement();
        Vector<Field> listField=this.getFields();

        String colonne="" , donnees="" , primaryKey=null;

        try {
            primaryKey=BddObject.getPrimaryKeyName(connection, table);
        } catch (Exception e) { }

        for (int j = 0; j < listField.size() ; j++) 
        {
            Field temp=(Field)listField.get(j);

            String d=this.valueForm(temp);
            if (primaryKey!=null && primaryKey.equals(this.columnNameAnnotation(temp))){
                d=this.setPrimaryKeyValue(connection, temp);
            }
            if (j!=listField.size()-1) 
            {
                colonne=colonne+this.columnNameAnnotation(temp)+",";
                donnees=donnees+d+",";
            }else{
                colonne=colonne+this.columnNameAnnotation(temp);
                donnees=donnees+d;
            }
        }
        String sql="INSERT INTO "+table+"("+colonne+")"+ " values ("+donnees+")";
        System.out.println(sql);

        statement.execute(sql);
        statement.close();
        if(createConnection == true ) connection.close();
    }

    public void update(Connection connection) throws Exception
    {
        String table_name = this.tableNameAnnotation();
        boolean createConnection=false;
        if(connection==null){
            createConnection = true;
            connection = Connect.getConnection();
        }

        Statement stat = connection.createStatement();
        String primaryKey = BddObject.getPrimaryKeyName(connection, table_name);
        Field objectField = this.getClass().getDeclaredField(this.pkFieldName());

        Method method = this.getClass().getMethod("get"+BddObject.changeFirstLetter(this.pkFieldName()));
        String primarykeyvalue = String.valueOf(method.invoke(this));

        Vector<Field> allObjectFields = this.getFields();
        String modification="";
        for (int i = 0; i < allObjectFields.size(); i++) 
        {
            Field temporaire = (Field)allObjectFields.get(i);            
            String donnee_temp =this.valueForm(temporaire);
            if (i != allObjectFields.size()-1) 
            {
                modification = modification+String.valueOf(this.columnNameAnnotation(temporaire))+"="+donnee_temp+",";
            }
            else{
                modification = modification+String.valueOf(this.columnNameAnnotation(temporaire))+"="+donnee_temp;
            }
        }

        String final_request = "UPDATE "+table_name+" SET "+modification+" WHERE "+primaryKey+" = "+"'"+primarykeyvalue+"'";
        System.out.println(final_request);
        stat.executeUpdate(final_request);
        stat.close();  
        
        if(createConnection == true ) connection.close();
    }

    public void delete(Connection connection) throws Exception
    {
        String table_name = this.tableNameAnnotation();
        boolean createConnection=false;
        if(connection==null){
            createConnection = true;
            connection = Connect.getConnection();
        }
        Statement stat = connection.createStatement();
        String primaryKey = BddObject.getPrimaryKeyName(connection, table_name);

        Method method = this.getClass().getMethod("get"+BddObject.changeFirstLetter(this.pkFieldName()));
        String primarykeyvalue = String.valueOf(method.invoke(this));


        String final_request = "DELETE FROM "+table_name.toUpperCase()+" WHERE "+primaryKey+" ='"+primarykeyvalue+"'";
        stat.execute(final_request);
        stat.close();
        System.out.println(final_request);
        if(createConnection == true ) connection.close();

    }

    public String valueForm(Field field)throws Exception{
        String getter="get"+BddObject.changeFirstLetter(field.getName());

        Method method = this.getClass().getMethod(getter);
        String data="'"+method.invoke(this)+"'";

        if (method.invoke(this) instanceof Number) {
            data=String.valueOf(method.invoke(this));
        }
        if(method.invoke(this) instanceof java.sql.Date){
            data="TO_DATE('"+ method.invoke(this) +"' , 'YYYY-MM-DD')";
        }
        if(method.invoke(this) instanceof java.sql.Timestamp){
            data="TO_TIMESTAMP('"+  method.invoke(this) +"' , 'YYYY-MM-DD HH24:MI:SS.FF')";
        }
        return data;
    }
    public String setPrimaryKeyValue(Connection connection , Field field)throws Exception{
        String setter="set"+BddObject.changeFirstLetter(field.getName());
        Method setPkey=this.getClass().getMethod(setter,field.getType());

        String sequence = this.getSequenceName();
        String pk = "";
        if(field.getType()==String.class){
            String value = this.pkString(connection, sequence);
            pk = "'"+value+"'";
            setPkey.invoke(this,value);
        }else{
            int nextVal = this.getNextVal(connection, sequence);
            pk=String.valueOf( nextVal );
            setPkey.invoke(this,nextVal);
        }
        return pk;
    }

    public String pkString(Connection connection , String sequence)throws Exception{
        String prefixe = this.pkPrefixe();
        String next_val = String.valueOf(this.getNextVal(connection, sequence));
        for (int i = prefixe.length() ; i < this.pkLongueur()-next_val.length() ; i++) {
            prefixe+="0";
        }
        return prefixe+next_val;
    }
    public int getNextVal(Connection connection , String sequence)throws Exception{
        String sql = "";
        if (connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("Oracle")) 
        {
            sql = "select "+sequence+".nextVal from dual";
        }
        if (connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("PostgreSQL")) 
        {
            sql = "select nextval('"+sequence+"')";
        }
        Statement state=connection.createStatement();
        ResultSet res=state.executeQuery(sql);
        res.next();
        int next = res.getInt(1);
        state.close();
        return next;
    }
    
    public Object resultSetToObject(Vector listField , ResultSet resultSet) throws Exception
    {
        Object objet=this.getClass().getConstructor().newInstance();
        for (int i = 0; i < listField.size(); i++) 
        {
            Field field=(Field)listField.get(i);

            String fieldType = BddObject.changeFirstLetter(field.getType().getSimpleName());
            String getterBase="get"+fieldType;

            String setterObject="set"+BddObject.changeFirstLetter(field.getName());

            Method getResultSet=ResultSet.class.getMethod(getterBase,String.class);
            Method setObjet=objet.getClass().getMethod(setterObject,field.getType());

            setObjet.invoke(objet,getResultSet.invoke(resultSet,this.columnNameAnnotation(field)));
        }
        return objet;
    }

    public static String changeFirstLetter(String mot)
    {
        char[] motChar=mot.toCharArray();
        char first=Character.toUpperCase(motChar[0]);
        motChar[0]=first;
        return String.valueOf(motChar);
    }
    
    public Vector getFields(){
        Vector<Field> listAttribut=new Vector<Field>();
        Field[] attributObject=this.getClass().getDeclaredFields();
        for (int i = 0; i < attributObject.length; i++) {
            if (attributObject[i].isAnnotationPresent(Colonne.class)) {
                listAttribut.add(attributObject[i]);
            }
        }

        return listAttribut;
    }
    // public Vector getFields(Connection connection , String table_name)throws Exception
    // {
    //     Statement statement=connection.createStatement();
    //     Vector<String> listColumnBase=BddObject.getAllColumnName(connection, table_name);

    //     Field[] attributObject=this.getClass().getDeclaredFields();
    //     Vector<Field> listAttribut=new Vector<Field>();
    //     for (int i = 0; i < attributObject.length; i++) 
    //     {
    //         if (listColumnBase.contains(attributObject[i].getName().toLowerCase())) 
    //         {
    //             listAttribut.add(attributObject[i]);
    //         }
    //     }
    //     statement.close();
    //     return listAttribut;
    // }
    // public static Vector getAllColumnName(Connection connection , String table) throws Exception
    // {
    //     String sql = "";
    //     if(connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("Oracle")){
    //         sql = "Select column_name from USER_TAB_COLUMNS where table_name='"+table.toUpperCase()+"'";
    //     }
    //     if(connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("PostgreSQL")){
    //         sql = "select COLUMN_NAME from information_schema.columns WHERE table_name='"+table.toLowerCase()+"'";
    //     }

    //     Statement stat=connection.createStatement();
    //     ResultSet resultSet=stat.executeQuery(sql);
       
    //     Vector<String> colonne=new Vector<String>();
    //     while(resultSet.next())
    //     {
    //         colonne.add(resultSet.getString("COLUMN_NAME").toLowerCase());
    //     }
    //     stat.close();
    //     return colonne;
    // }
    public boolean sequenceExist(Connection connection) throws Exception
    {
        String sequence=this.getSequenceName();

        String sql = "";
        if (connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("Oracle")) {
            sql = "Select * from user_sequences where sequence_name='"+sequence.toUpperCase()+"'";
        }
        if (connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("PostgreSQL")) {
            sql = "select * from information_schema.sequences where sequence_name='"+sequence.toLowerCase()+"'";
        }
        boolean exists=false;
        Statement state=connection.createStatement();
        ResultSet res=state.executeQuery(sql);
        try {
            if (res.next()) {
                exists=true;
            }
        } catch (Exception e) {}
        state.close();
        return exists;
    }
    

    public static String getPrimaryKeyName(Connection connection , String table_name)throws Exception
    {
        String sql = "";     
        if(connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("Oracle"))
        {
            sql = "SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '"+table_name.toUpperCase()+"' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position";
        }
        if (connection.getMetaData().getDriverName().split(" ")[0].equalsIgnoreCase("PostgreSQL")) 
        {
            sql ="SELECT a.attname column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid =i.indrelid AND a.attnum= ANY (i.indkey) WHERE i.indrelid='"+table_name+"'::regclass AND i.indisprimary";
        }
        Statement stat=connection.createStatement();
        ResultSet resultSet = stat.executeQuery(sql);
        resultSet.next();
        String primaryKey = "";
        try{
            primaryKey = resultSet.getString("column_name").toLowerCase();
        }catch(Exception e){
            throw new Exception("La table '"+table_name+"' n'a pas de primary key");
        }finally{
            stat.close();
        }
        return primaryKey;
    }

    public String getSequenceName(){
        String sequence = "";
        Table table = this.getClass().getAnnotation(Table.class);
        if(table.sequence().isEmpty()){
            try{
                sequence =  this.tableNameAnnotation()+"_seq";
            }catch(Exception e){}
        }else{
            sequence = table.sequence();
        }
        return sequence;
    }

    public String columnNameAnnotation(Field f){
        String colonne_name = f.getName();
        Colonne colonne = f.getAnnotation(Colonne.class);
        if(!colonne.name().isEmpty()){
            colonne_name = colonne.name(); 
        }
        return colonne_name;
    }
    
    public String tableNameAnnotation()throws Exception{
        if(this.getClass().isAnnotationPresent(Table.class)){
            Table table = this.getClass().getAnnotation(Table.class);
            return table.name();
        }else{
            throw new Exception("Nom de Table Introuvable");
        }
    }

    public String pkPrefixe()throws Exception {
        Field field = this.pkField();
        String pkPrefixe = "";
        Colonne colonne = field.getAnnotation(Colonne.class);
        if(colonne.pkPrefixe().isEmpty()){
            pkPrefixe = this.getClass().getSimpleName().substring(0, 3).toUpperCase();
        }else{
            pkPrefixe = colonne.pkPrefixe();
        }
        return pkPrefixe;
    }

    public int pkLongueur() throws Exception{
        Field field = this.pkField();
        Colonne colonne = field.getAnnotation(Colonne.class);
        return colonne.pkLongueur();
    }


    public Field pkField()throws Exception{
        Vector<Field> fields = this.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Colonne colonne = fields.get(i).getAnnotation(Colonne.class);
            if(colonne.isPrimaryKey()){
                return fields.get(i);
            }
        }
        throw new Exception("Erreur");
    }

    public String pkFieldName()throws Exception{
        Vector<Field> fields = this.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Colonne colonne = fields.get(i).getAnnotation(Colonne.class);
            if(colonne.isPrimaryKey()){
                return fields.get(i).getName();
            }
        }
        throw new Exception("Erreur");
    }
}
