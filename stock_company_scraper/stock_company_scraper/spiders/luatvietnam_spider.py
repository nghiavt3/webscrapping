import scrapy
import sqlite3
from stock_company_scraper.items import EventItem
from datetime import datetime
import re
class EventSpider(scrapy.Spider):
    name = 'event_luatvietnam'
    mcpcty = 'LUATVIETNAM' 
    allowed_domains = ['luatvietnam.vn'] 
    count = 0
    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.db_path = 'stock_events.db'
        self.max_pages = 3

    async def start(self):
        urls = [
            ('https://luatvietnam.vn/van-ban-moi.html', self.parse_generic),
        ]
        
        raw_cookie = "_hjSessionUser_5192214=eyJpZCI6IjM4MTI2ZGFkLTY2NGUtNTBmZS1iYzdjLTBlYzJiMjY3YzVkMCIsImNyZWF0ZWQiOjE3NDI1MTc4MDU0NDQsImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.1389618940.1770341074; _fbp=fb.1.1770341110241.360692750843063865; _gta_uni=823749984.397955804.094934686275; __vnp_guest_id=397955804; __gads=ID=d3f9bbffc7f36f2a:T=1742517805:RT=1770341773:S=ALNI_MbuVYaSn5xBgAxB0o8LcQIxs6SfkQ; __gpi=UID=0000106bc0fda70c:T=1742517805:RT=1770341773:S=ALNI_MZ69FBFRhtiVjl4wDRg_SJ-ICxAwQ; __eoi=ID=c6845146a01dda08:T=1770341773:RT=1770341773:S=AA-AfjaaVvawegirwRb5DRNlU3UD; _uidcms=1770341795999254867; __RC=65; __R=3; __uif=__uid%3A165119582490880405%7C__ui%3A-1%7C__create%3A1770341796; __tb=0; __IP=0; cto_bundle=s4gF6l9FUWNGc0RBZUVVTkxRSkIlMkI3d09MS1ZCTmJhWU9EYVZhOGRub2xNU29kSVd1YTcydHUzY2tSRWx0b1FyRVlmJTJCOFNBZE5OTXFvWEFZWno4N1FuY2JRejhvVzBjRVRFa0pPNzMyNXA1QnJBWFhLRFp4SDlyS1dFRyUyQmtGMjJndWVhYVhpZjVoTTR5d0NMNWl1MGg0V01nVFElM0QlM0Q; FCNEC=%5B%5B%22AKsRol8zm9mOSoiiTi3mmpBLqTxD73bGutTD6d7JM1kxHBQcX2EjLwWDGI-9l8uSS5xV2hdNJMl8YcpV1nA029maDzarKajYnOSuTSpxhjIVloU-UpPXyiOAuJc_tg0F_pq-yXbvmt61U7cdft9v_m7IMPoGdh1EKQ%3D%3D%22%5D%5D; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%22aa6c36d0-1e3f-4d2c-aea4-5bd8467345c9%5C%22%2C%5B1770341774%2C255000000%5D%5D%22%5D%5D%5D; .AspNetCore.Antiforgery.TTwcN29AroI=CfDJ8D4lKE5Sh3RErO920jzumo3OmS5nmFYpJcCC_B0trSpgiq0Ic_eJ0aOEJxTg7H1DHHv6ZQVa9M6EuCEj_xNr0rT4b5YLVd6KjOeUUr3lLluay9mDboywUlRLWbjuNbRRZ0lYleRmxsC0RFK-IvKuha0; _ga=GA1.1.849318487.1770341074; _ga_EW2DZ6FMZM=GS2.2.s1770367541$o3$g1$t1770374398$j1$l0$h0; _ga_ZWHEHZ2EHL=GS2.1.s1770377197$o4$g0$t1770377197$j60$l0$h2011556537; ASP.NET_SessionId=c5evmpr0giksaoqqjysp3i3y; .LuatVietNamSSO=CfDJ8D4lKE5Sh3RErO920jzumo2T6wYhwnxtsSifl2-fEsJh712hql6C3aRgGAdGWxwDhbGMeukErDjiTXobpmuncVihi5-pt1Xy61XuWLW1YAWYgOcB2OiFeUeXrff-gnxG6ott2NiqDTerYnBagaZbP5SpfM1Bhsvcqm1xNK9BMWP1wscBp4IPmshpm6shvn9B4yJVSEyMe61uxdmq8ybD65r5hMzbJ9Sgt-5yOxSdQ-vYbLzcRsCDVqTuyg-6iq6Fe_BsKcmB0KnCi7PVjm6lb6fNrkdEVDRIuwOGIhCZX_yiPDgO1dn3uXv_rQ2fRYO2LeQZT07jStBTQ5_xdqw5OSL_g64vJqCEcvLwpz0LkrORfVBbbzTHAn04KZsttASQz_3brLY3aBGickDeM84RlWbaL3c15Z6jDzPB53HTPINtLUcvzn2zYQnfcnIb-Z7L3ZWx2MpzmbS65oZaj373V-0J45P7_FKn17e5EL40YmrloFGm8tlZnuCoUcm4mIrXmd0ADaeCEtYIgcbYkmJqE7LUViEEeN8d-Vq8uS_p-YHR5WCVudFq5PgWKvFwIzNGGeA-75MZac7LGPkFaYsFErA_OVU_Qhvc6EUvixmvqYLKZIlV3WCakvHjuboLuKXGnaPwXc-0VGciTYENQ59jpuhzv4r_QUUhFDDvwgNr-7Xicog_Zzn066m22UYRpruga3euwb6lfIb1WHL_o_mlqZTC_edNFi9IUQFZxlC6oofh75OukneO8pcZPSoPSGzz5IpGIac0dKkPWJ43P3u5jgC4m5kS8Cc09rqJibJXC3-ZAZonK3efb9kP_RRbEnKkFYZepzbaM9pnEyM3HYFvoM0LI9umAszJR-8Luphe80v9_Jxv_kjb6ctOHCd067fTymGZiEtCsdb5-avzMjW8E8bBTox2DEs_jH59qcP6gQYyxDzTrXstn17GH0CBpxJP2Sqnc0NF5Ztk9DuRt2w7HFS2bZb3mfUdb4c-I5xu1U2ib1WyP7bTXtVPW0Cf226R7FrH3ohvdi7oCxQ_ju3wiZCjPulShnFE8iIcgHZfAg4e-rEAx7XX9I4oWFTIAOwwRA-qQaXy1fkQWBWHpOVRzi-ytQ3E4K9Ws6ITluX0Lhw0AgCl7YqZ6VXAPmi2Yy91T_AIHylUX6ElyU9pBD3NCFZSSASvOGY-rOvb9QdWYtQ4NpzOvtWpo1Qv93sWU7jzKUikyrrRkzm40dja101h1NkyEsj6Df8BqRwKZNSr513ZG_X8w45uKlKpPaWNJvO0ncWxR7cX_mIPO-lIvKDXLMJtugKlN0s41e64Pa9XXKrIX9v9X5QrR9oOQm45XReaLq6aoECxtAj8PjRjrHTktrnske8K9Kz-Ia5L_3TNvL4bpJHc4FXSgz_es7-ONVrjqxMpIZ0YpDr3K3dM8v_AtsoZVeshUZ_kYGMlm4XV1HRZ0YLHQ-dimSHHkNlADX6p8_c-Aj1S9Yr6cBWZ3ghi_I4NPoGVj4Ci-OirafpL_rU_JTH7ZqnlSeLkIM4g-uMmjAAw6a2kmzkcs8EUatBk_5VSaOrVmGo3_28E_IcTS5wl11bdju6ia_FSqkLKtpOKZk6BBcnd8Ht1Qf5Z3vhGssmYw4OsBYze-9uoeVX8m3SSH1kcsTdq6k0xEO-o17l7Xuh7Ta8CQgb_RRvftFWKV3010IDpNL7WVzQRBN4GEGqX03biXYP59Fe8-G8qPebqJZL4PlTBoesZY1kJf6MKBNVkhsVpjZhh5dqkkozUPMnZ8O7D8VEF9QlPya-mq2uZ6k1Z7FRa9xFoS1u_901nrubqgUZZwZxsgCZyA28NgxGbcu85fc6mh7A2KeOOKwmOObsheRClOThxu513Kdf2iBX4v7EVYf7nXeNYHtpVJ2rC; LuatVietnamShare=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJDdXN0b21lcklkIjoiMTI0MjY0MiIsIlBhcnRuZXJJZCI6IjAiLCJTZXNzaW9uSWQiOiJlNmFhNTEzNi0zN2NhLTQ5OWUtYWI2Mi00YjBhMWY5OGRiNmMiLCJFeHBpcmVBdCI6IjIwMjYtMDUtMDdUMTE6Mjk6MDYuNzU5MzM3N1oiLCJleHAiOjE3NzgxNTMzNDYsImlzcyI6Imh0dHBzOi8vYWlsdWF0Lmx1YXR2aWV0bmFtLnZuLyJ9.nJ4z55dgCtMl_8hpJSL0GOCTPvGS1RIoYHHjnMM4b_4; LAWSVN_AUTH=43A47C68589DBF47C38C4C1F651C24354667C149EF667A345721CA7DB46AB656F34ECE9E49A362173FDDB6FD993064DF2707BCCEAAD90A4C669F7DEDB5A13FB43D1178ED5E0DDBEE5F64DF3A0427C70903B593D912B6410BB4617F4498C040EBFDB0AD386F2FA9B9DF50BD676F4F64E71DF66C48DE025DE63EE6D53E70B46B4ED17994FA83EAE55BB73909DAEBBA3777ECC0A4DA5A656260C00E100C0B4AFD0604DC58B930846F664A66008F09834A5FA6B2984FD45050A4A71FA002856277B87B29AC0F02B8714703F745A0BF55651FFDC5ED389BE60E1F3D76C4F4B8C00BFB64E947F07BDB53A0D9263741CE5180AC94F6E0C682F0AB2A8AFC291438D089EA2AC67A525D885D843F90E837BE78DD54DAE8100F011219EE86FCE611CA9AF2DE597671FA2FCC2EDC2B53294B84BE613644918526C40051DEB54FB5A6610F0D4BCBA057D1A3E656458372F19072A7F7CDE3D0A5C94E02AB2F000A83AF4ABA23F499788DB0497B0B2E9BA09C33E28ED171CA91546902D958BFBD6F77A9EDE0350B7B4EB74C7730268FC2B3579E5C6B57C7BA9A125BB4F2200B3F14EC740D1F16C683965A82B48040CD31D07CBA10403A99DA6542483666992DF0DD228D537A129DC2A87F0AD9E3335E596A26787157249A45BE6E248B2102E84F0AAE83EE3CCC1077915DD5F986C64827E61B45F6CB83150B33A5DCB24AF1FFB72B9B98DAAAE508AB7FFFBF771C51E46765764BC5F391ECD92137A599C185B589EDCCAD2F1236B52A4002DCBD7FB11529428211316A0A44CE43342545F22DECE361906E6F0DD90268900F796752E655695819626ADDE8FDC6A3819279834EBFECA8A51596C5CB28F1276BCA005B1BCA1380DC49084BEBB697A0F8EB450AF9B24744856C1593C2D3DC51E4DF4E03C91C18F1134C81221707C04FAB2AA4ECBF7C5689BFA1D594D11BC22F0DD10218CE3280937000A42AA29D1AD217CBBD13D2A9CE54C5C7853B266D5421CD9380B42FBB65E955D553B30414DA95FB472941DF2A71A0B8903CD16F814C23F93746370696043B94E98ACE03973CF96EB5D17362AFAE5BD1D92E9FE166871FAB31C73E69F49B904109DBED05D50A3140BA9EFE1000F332ABD63C5565119ED4CC3103ED07DDDD6FC5EE2CCBB90DD29A91FEFE38DEED1F38A6876D7AFEE485AE17E9C62193AF348E5C96BE2F2A5BC5D9881728170D210AF6FFD7B022E75F5B8776A17C353FAF9451AD988A49C3562411FDF011E608AAE6EC889C20525CAD2CA00677D47186DA6C76CDDC1BDB9B9DE1446E5C1849F15F26483597BBC811625D3F1B9E0765C908DA915E2D657D612A0A26B92400C6BADA6DE5824A7CFA44374D9F2843CCA31EFDB510F352FDFAEAAB570ECF85E128A1C40341911F82769CF430D64A3F3EAEFA1F16107D41656D3602F1964A261EE19E547FC2149826620AA1056E168D1A26F8CB375B757D84F783D767A11D159393933FE34C9B4E20AC0E31A8217D5CB4828D9DD840FBB0C58680FEF4343721F0142FDB3BB96D380F185097E46BE5154AB5285E7E924127F3C238E259EDDA7C1FD816F6BB6E92AD69091DF3F546AA8B5F47B52134D36952967716F081362ABDF0830631D68484A0BA83D4EB923BC4B7B934A6856C3C11BE71EE54B909E46E68BBEA9ECE367F7F8B8C543E4F3EA30E9DBF2BE9CB16E93746D5F9B3B167E15EE413EC40F9B7FAED2582E6479864231280710C57D7A7FA35CDBBB30275BFA8AE62DF52EB5A5DC92E2A373810322E4AFA98CC9AAD5E8C8237E29B0FDBA992EDFD1B3404296A124CB7244D15EDC619BB03B27134C34094976FEF90D09D1EB91E799F712CCF55FAB937A28C25872B73359CF26E49C93AB2BC97EAF050914E9225FCB005089EC20A13B245C98BB0E28DEF99C82331BCF6A86CD873C7C5A022A31D1F799A340B3424F0C454E5433F8970338EF04ED0F1F67E99144642E454FD64B35AECE09B4A7647016D56527EFA8B0AA58BDF3A203D74B881C5C12A1054283F66839F319DA1E2A35A77FD24E5555DC3A6F025B4701377009D63BA471AB63B659F355F5A5F781CE508536FC5CD12763E838EEDAEA16824986992CD66059B66CA9A2469D4B4E68A7B5D3680578C1950BB874DA4B07F5988B7D5132DDB24FBC018D42D31E26AC31881547EB170F7E0F4535DDB6FE50F20886CDCC2B821FD721722030A2F7D690EE6F143E720F93F7655FC438196C4981E03926E0CCC18ABD788A995D5670BCFD67FF19543809DC90E68822128D84078A9D2CD1309AE5B41DBC87B034B3C0DB7E1F4E5634CE42DED4B96CCD5003C245AC3652AC95A71D7B2C7F6083C7401C7BF0121BFE0B3FAAA0084B21F8BA8976B865348A409B4FF87551B72B0B20BAF3C32225596EA57F0F2A141E345AAA30C9E12CBCBD67223E628BCF78022E6EF405302074FDA46D24E6A24113CC1690539DCE93C322AEC32748180ADC1ECE934018E5BCDEC436FC2519361828A1607AD1A2A39ECAF8AAF5EA258346E80A880DD69868716442A2C2AA72D43496FF794DC4E098D99E5BED0BED70DB96B599C9FED6C603A0CFABD4C6DD86DA916AAEFDF510FFB09B6D61626BACEB8658115941194AE; _ga_2GQESC9SL5=GS2.1.s1770377197$o4$g1$t1770377374$j5$l1$h965887810"
        
        # Chuyển chuỗi thành dictionary
        cookie_dict = {i.split('=', 1)[0].strip(): i.split('=', 1)[1].strip() for i in raw_cookie.split(';')}
        for url, callback in urls:
            yield scrapy.Request(
                url=url, 
                cookies=cookie_dict,
                callback=callback,
                meta={'page_count': 1} # Khởi tạo đếm trang từ trang 1
            )

    async def parse_generic(self, response):
        current_page = response.meta.get('page_count', 1)
        self.logger.info(f"--- ĐANG XỬ LÝ TRANG SỐ: {current_page} ---")
        """Hàm parse dùng chung cho các chuyên mục của SeABank"""
        # 1. Khởi tạo SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table_name = f"{self.name}"
        #cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY, mcp TEXT, date TEXT, summary TEXT, 
                scraped_at TEXT, web_source TEXT, details_clean TEXT
            )
        ''')
        # Lấy tất cả các hàng trừ hàng tiêu đề năm
        # Lặp qua từng bài viết văn bản
        articles = response.css('article.doc-article')
        stop_pagination = False
        for article in articles:    
            title_link = article.css('.doc-title a')
            title = title_link.attrib.get('title')
            link = response.urljoin(title_link.attrib.get('href'))
            date_published = article.xpath('.//div[@class="doc-dmy"][contains(., "Ban hành") or contains(., "Xác thực")]//span[@class="w-doc-dmy2 color-law"]/text()').get()  
            # Trích xuất ngày cập nhật (thường nằm trong div ẩn m-hide)
            date_updated = article.xpath('.//div[contains(@class, "doc-dmy") and contains(., "Cập nhật")]//div[@class="w-doc-dmy2 color-law"]/text()').get()
            
           
            if not title:
                continue

            summary = title.strip()
            iso_date = convert_date_to_iso8601(date_published)
            absolute_url = f"{response.urljoin(link)}"

            # -------------------------------------------------------
            # 3. KIỂM TRA ĐIỂM DỪNG (INCREMENTAL LOGIC)
            # -------------------------------------------------------
            # Regex trích xuất phần đầu đến chữ "của" hoặc "do"
            match = re.search(r'^(.*?)\s+(của|do)\b', summary, re.IGNORECASE)
            prefix = match.group(1).strip() if match else summary[:30].strip()
            
            # Làm sạch prefix: bỏ ký tự đặc biệt, thay khoảng trắng bằng gạch dưới
            clean_prefix = re.sub(r'[^\w\s\-/]', '', prefix)
            event_id = f"{clean_prefix}_{iso_date if iso_date else 'NODATE'}".replace(' ', '_').strip()
            
            cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (event_id,))
            if cursor.fetchone():
                self.logger.info(f"===> GẶP TIN CŨ: [{summary}]. DỪNG QUÉT CHUYÊN MỤC.")
                stop_pagination = True
                break 

            # 4. Yield Item
            e_item = EventItem()
            e_item['mcp'] = self.mcpcty
            e_item['web_source'] = self.allowed_domains[0]
            e_item['summary'] = summary
            e_item['date'] = iso_date
            e_item['details_raw'] = f"{summary}\nLink:"
            e_item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if absolute_url :
                yield scrapy.Request(
                    url=absolute_url,
                    callback=self.parse_detail,
                    meta={'item': e_item}  # Chuyển dữ liệu sang hàm tiếp theo
                )
            else:
                yield e_item 
        conn.close()
        
        # --- LOGIC PHÂN TRANG (PAGINATION) ---
        # Chỉ đi tiếp nếu chưa gặp tin cũ VÀ số trang hiện tại < max_pages (3)
        if not stop_pagination and current_page < self.max_pages:
            next_page_link = response.xpath('//span[contains(@class, "active")]/following-sibling::a[1]/@href').get()
            if next_page_link:
                next_page_url = response.urljoin(next_page_link)
                yield response.follow(
                    next_page_url, 
                    callback=self.parse_generic,
                    meta={'page_count': current_page + 1} # Tăng số trang lên
                )
            else:
                self.logger.info("Đã hết trang để quét.")
        else:
            self.logger.info(f"Dừng tại trang {current_page} do đạt giới hạn hoặc gặp tin cũ.")

    def parse_detail(self, response):
        # Nhận lại item từ trang danh sách gửi qua meta
        item = response.meta['item']
        
        # 1. Trích xuất tất cả các link PDF trong khối chi tiết
        # Chúng ta tìm các thẻ <a> nằm trong .blog-details-col có href chứa ".pdf"
        download_pdf_url = response.css('.list-download a[title*="PDF"]::attr(href)').get()
        if download_pdf_url :   
            item['details_raw'] = f"{item['details_raw']}\n {response.urljoin(download_pdf_url)}"
            yield item
        else :
            item['details_raw'] = f"{item['details_raw']}\n {response.url}"
            yield item
    
def convert_date_to_iso8601(vietnam_date_str):
    if not vietnam_date_str:
        return None
    try:
        date_object = datetime.strptime(vietnam_date_str.strip(), '%d/%m/%Y')
        return date_object.strftime('%Y-%m-%d')
    except ValueError:
        return None