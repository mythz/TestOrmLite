using ServiceStack.DataAnnotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestOrmLite
{
    [Alias("UserAuth")]
    [CompositeIndex(true, "CompanyId", "UserName")]
    public class MyCustomUserAuth 
    {
        [AutoIncrement]
        public int Id { get; set; }

        [References(typeof(Company))]
        public long CompanyId { get; set; }

        public string UserName { get; set; }

        public string Email { get; set; }
    }

    public class Company
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

}
