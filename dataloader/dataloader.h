#include<bits/stdc++.h>
using namespace std;

enum class OpeType{
    INSERT,
    UPDATE,
    READ,
    SCAN
};

struct OpeEntry
{
    OpeType type;
    std::string key; //key值
    std::string value; //value值
    OpeEntry(): key(),value()  {}

    std::string toString() const{
        std::string str = "type:" ;
        if(type == OpeType::INSERT){
            str +="INSERT";
        }
        else if(type == OpeType::UPDATE){
            str +="UPDATE";
        }
        else if(type == OpeType::READ){
            str +="READ";
        }
        str +=",key"+key+",value"+value;
        return str;
    }
    

};

//ycsb load 数据
class YCSBDataLoader{
    private: 
        FILE *fin;
    public:
        YCSBDataLoader() : fin(nullptr){}


        ~YCSBDataLoader()
        {
            if(fin) fclose(fin);
        }

        void setFilePath(const std::string & new_file_path)
        {
            bool reset = checkValidPath(new_file_path);
            if(!reset) {} //抛出异常

            if(fin) fclose(fin);
            fin = fopen(new_file_path.c_str(),"r");


        }

        bool hasNext() //判断是否到文件结尾
        {
            return !feof(fin);
        }

        //获取一条记录
        OpeEntry getone()
        {
            //to do 异常处理
            if(!fin) std::cout<<"please specify a file to open" ;
            char *line = nullptr;
            size_t len = 0;
            if(getline(&line,&len,fin) == -1){
                free(line);
                //to do 异常
            }

            OpeEntry entry = parseoneline(line);
            free(line);
            return entry;

        }

        //获取所有entry
        std::vector<OpeEntry> getall()
        {
            std::vector<OpeEntry> Opeentries;
            while(hasNext()){
                Opeentries.push_back(getone());
            }

            return Opeentries;
        }

    private:
        static bool checkValidPath(const std::string &file_path)//判断路径是否有效
        {
            std::ifstream fin(file_path);
            bool reset = fin.is_open(); //判断打开是否正常
            fin.close();
            return reset;
        }

        static OpeEntry parseoneline(const std::string &line)//解析一行
        {
            size_t index = line.find_first_of(' '); //找到第一个空格的下标
            if(index ==std::string::npos){
                std::cout<<"not a standard record"<<std::endl;
            }

            std::string op = line.substr(0,index);//将第一段字符转大写

            std::transform(op.begin(),op.end(),op.begin(),::toupper);
            OpeEntry opeEntry;
            if(op=="INSERT"){
                opeEntry = parseInsert(line);
                opeEntry.type = OpeType::INSERT;
            }
            else if(op=="UPDATE"){
                opeEntry = parseInsert(line);
                opeEntry.type = OpeType::UPDATE;
            }
            else if(op=="READ"){
                opeEntry = parseRead(line);
                opeEntry.type = OpeType::READ;
            }

            return opeEntry;

        }


        static OpeEntry parseInsert(const std::string &line) //解析每个insert 的record
        {
            static int key_prefix_len = strlen("INSERT usertable user");
            static int value_prefix_len = strlen("[ field0=");
            OpeEntry entry;
        //key开始的位置
            int key_start_pos = key_prefix_len;
        //key结束的位置，   find_first_of找到key之后的第一个空格符
            int key_end_pos = line.substr(key_start_pos).find_first_of(' ') + key_start_pos-1;

            //获得key
            entry.key = line.substr(key_start_pos,key_end_pos-key_start_pos+1);
            //定位到value第一个字符的位置
            int value_start_pos = key_end_pos+1+value_prefix_len+1;
            int value_end_pos;
            
            value_end_pos = line.size()-3;

            entry.value = line.substr(value_start_pos,value_end_pos-value_start_pos+1);
            return entry;
 
        }

        static OpeEntry parseRead(const std::string &line)//解析读操作
        {
            static int key_prefix_len = strlen("READ usertable user");
            OpeEntry entry;
            int key_start_pos = key_prefix_len;
            int key_end_pos = line.substr(key_start_pos).find_first_of(' ') + key_start_pos-1;
            //获得key
            entry.key = line.substr(key_start_pos,key_end_pos-key_start_pos+1);

            return entry;

        }





    




};





