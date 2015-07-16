package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    private HashMap<String, Integer> nameIdMap;
    private HashMap<Integer, String> idNameMap;
    private HashMap<Integer, DbFile> idFileMap;
    private HashMap<Integer, String> idPkeyMap;
    
    /**
     * Constructor. Creates a new, empty catalog.
     */
    public Catalog() {
        nameIdMap = new HashMap<String, Integer>();
        idNameMap = new HashMap<Integer, String>();
        idFileMap = new HashMap<Integer, DbFile>();
        idPkeyMap = new HashMap<Integer, String>();
    }

    /**
     * Add a new table to the catalog. This table's contents are stored in the
     * specified DbFile.
     * 
     * @param file
     *            the contents of the table to add; file.getId() is the
     *            identfier of this file/tupledesc param for the calls
     *            getTupleDesc and getFile
     * @param name
     *            the name of the table -- may be an empty string. May not be
     *            null. If a name conflict exists, use the last table to be
     *            added as the table for a given name.
     * @param pkeyField
     *            the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        int id = file.getId();
        nameIdMap.put(name, id);
        idNameMap.put(id, name);
        idFileMap.put(id, file);
        idPkeyMap.put(id, pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog. This table has tuples formatted using the
     * specified TupleDesc and its contents are stored in the specified DbFile.
     * 
     * @param file
     *            the contents of the table to add; file.getId() is the
     *            identfier of this file/tupledesc param for the calls
     *            getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        Integer id = nameIdMap.get(name);
        if (id != null) {
            return id;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId()
     *            function passed to addTable
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        DbFile file = idFileMap.get(tableid);
        if (file != null) {
            return file.getTupleDesc();
        } else {
            throw new NoSuchElementException("table doesn't exist.");
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the specified
     * table.
     * 
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId()
     *            function passed to addTable
     * @throws NoSuchElementException
     *            if the table doesn't exist
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        DbFile file = idFileMap.get(tableid);
        if (file != null) {
            return file;
        } else {
            throw new NoSuchElementException("table doesn't exist.");
        }
    }

    public String getPrimaryKey(int tableid) throws NoSuchElementException {
        String pkey = idPkeyMap.get(tableid);
        if (pkey != null) {
            return pkey;
        } else {
            throw new NoSuchElementException("table doesn't exist.");
        }
    }

    public Iterator<Integer> tableIdIterator() {
        return idFileMap.keySet().iterator();
    }

    public String getTableName(int id) throws NoSuchElementException {
        String name = idNameMap.get(id);
        if (name != null) {
            return name;
        } else {
            throw new NoSuchElementException("table doesn't exist.");
        }
    }

    /** Delete all tables from the catalog */
    public void clear() {
        nameIdMap.clear();
        idNameMap.clear();
        idFileMap.clear();
        idPkeyMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(
                    catalogFile)));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type,
                // ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1,
                        line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name
                        + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema "
                        + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
