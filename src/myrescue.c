/*
    myrescue Harddisc Rescue Tool
    Copyright (C) 2002  Kristof Koehler (kristofk at users.sourceforge.net)
                        Peter Schlaile  (schlaile at users.sourceforge.net)

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#define __USE_LARGEFILE64 1
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <string.h>

long long filesize ( int fd )
{
	long long rval = lseek64(fd, 0, SEEK_END) ;
	if (rval < 0) {
		perror("filesize");
		exit(-1);
	}
	return rval;
}

int peek_map(int bitmap_fd, int block)
{
	char c = 0;
	if (lseek64(bitmap_fd, block, SEEK_SET) < 0) {
		perror("peek_map lseek64");
		exit(-1);
	}
	if (read(bitmap_fd, &c, 1) < 0) {
		perror("peek_map read");
		exit(-1);
	}
	return c;
}

void poke_map(int bitmap_fd, int block, char val)
{
	if (lseek64(bitmap_fd, block, SEEK_SET) < 0) {
		perror("poke_map lseek64");
		exit(-1);
	}
	if (write(bitmap_fd, &val, 1) != 1) {
		perror("poke_map write");
		exit(-1);
	}
}

int copy_block( int src_fd, int dst_fd, 
		long block_num, int block_size,
		unsigned char * buffer )
{
	long long filepos ;
	ssize_t src_count ;
	ssize_t dst_count ;

	filepos = block_num;
	filepos *= block_size;

	if (lseek64(src_fd, filepos, SEEK_SET) < 0) {
		perror("lseek64 src_fd");
		return errno;
	}

	if (lseek64(dst_fd, filepos, SEEK_SET) < 0) {
		perror("lseek64 dst_fd");
		return errno;
	}

	src_count = read(src_fd, buffer, block_size);
	if (src_count != block_size) {
		if (src_count == -1) {
			perror("src read failed");
			return errno;
		} else {
			fprintf(stderr,"short read: %d of %d\n",
				src_count, block_size);
			return -1;
		}
	}
		
	dst_count = write(dst_fd, buffer, block_size);
	if (dst_count != block_size) {
		if (dst_count == -1) {
			perror("dst write failed");
			return errno;
		} else {
			fprintf(stderr,"short write: %d of %d\n",
				dst_count, block_size);
			return -1;
		}
	}
	return 0;
}

int try_block ( int src_fd, int dst_fd, 
	       long block_num, int block_size, int retry_count,
		unsigned char * buffer )
{
	int r ;
	for ( r = 0 ; r < retry_count ; r++ ) {
		if ( copy_block ( src_fd, dst_fd, 
				  block_num, block_size,
				  buffer ) == 0 )
			return 1 ;
	}
	return 0 ;
}

int check_block ( int bitmap_fd, long block_num, 
		  int good_range, int failed_range,
		  int skip_fail,
		  long start, long end )
{
	int range = (good_range > failed_range) 
		? good_range : failed_range;
	int found_good = 0;
	long b;
	for ( b = block_num - range; b <= block_num + range; b++ ) {
		if ( (b < start) || (b >= end) )
			continue;
		char st = peek_map(bitmap_fd,b);
		if ( (failed_range > 0) &&
		     (abs(block_num-b) <= failed_range) &&
		     (-st >= skip_fail) )
			return 0;
		if ( (good_range > 0) &&
		     (abs(block_num-b) <= good_range) &&
		     (st > 0) )
			found_good = 1;
	}
	return good_range > 0 ? found_good : 1;
}

void print_status ( long block, long start_block, long end_block, 
		    long ok_count, long bad_count )
{
	fprintf ( stderr, 
		  "\rblock %09ld (%09ld-%09ld)   "
		  "ok %09ld   bad %09ld   ",
		  block, start_block, end_block,
		  ok_count, bad_count ) ;
}

void do_copy ( int src_fd, int dst_fd, int bitmap_fd,
	       int block_size, long start_block, long end_block,
	       int retry_count, int abort_error, int skip, 
	       int skip_fail, int reverse,
	       int good_range, int failed_range,
	       unsigned char * buffer )
{
	long block_step = 1;
	long block ;
	long ok_count = 0 ;
	long bad_count = 0 ;
	char block_state ;
	int  forward = !reverse;

	for ( block = forward ? start_block : (end_block-1) ; 
	      forward ? (block < end_block) : (block >= start_block) ; 
	      block += forward ? block_step : -block_step ) {
		block_state = peek_map ( bitmap_fd, block ) ;
		if ( (block_state <= 0) &&
		     ( (skip_fail == 0) || (-block_state < skip_fail) ) &&
		     check_block ( bitmap_fd, block,
				   good_range, failed_range, skip_fail,
				   start_block, end_block ) ) {
			print_status ( block, start_block, end_block, 
				       ok_count, bad_count ) ;
			if ( try_block ( src_fd, dst_fd, 
					 block, block_size, retry_count,
					 buffer ) ) {
				++ok_count ;
				poke_map(bitmap_fd, block, 1);
				block_step = 1;
			} else {
				++bad_count;
				poke_map(bitmap_fd, block, block_state-1);
				if (abort_error)
					break;
				if (skip)
					block_step *= 2;
			}
		} else {
			if ( block % 1000 == 0 ) {
				print_status ( block, start_block, end_block, 
					       ok_count, bad_count ) ;
			}
			block_step = 1 ;
		}
		
	}
	print_status ( forward ? end_block : start_block, start_block, end_block, 
		       ok_count, bad_count ) ;
	fprintf ( stderr, "\n" ) ;
}

int do_jump_run ( int src_fd, int dst_fd, int bitmap_fd,
		  int block_size, long start_block, long end_block,
		  int retry_count, int abort_error, int skip, 
		  int skip_fail, int jump,
		  int good_range, int failed_range,
		  long block, int jump_count, int jump_step,
		  long *ok_count, long *bad_count,
		  unsigned char * buffer ) 
{
	char block_state ;
	for ( ; jump_count-- > 0 ; block += jump_step ) {
		if ( block >= end_block )
			break;
		if ( block < start_block )
			break;
		
		block_state = peek_map ( bitmap_fd, block ) ;
		if ( block_state > 0 )
			continue;
		
		if ( ((skip_fail > 0) && (-block_state >= skip_fail)) ||
		     (!check_block ( bitmap_fd, block,
				     good_range, failed_range, skip_fail,
				     start_block, end_block )) ) {
			if (skip || abort_error)
				return 0;
			else
				continue;
		}

		print_status ( block, start_block, end_block, 
			       *ok_count, *bad_count ) ;
		
		if ( try_block ( src_fd, dst_fd, 
				 block, block_size, retry_count,
				 buffer ) ) {
			++(*ok_count);
			poke_map(bitmap_fd, block, 1);
		} else {
			++(*bad_count);
			poke_map(bitmap_fd, block, block_state-1);
			if (skip || abort_error)
				return 0;
		}
	}
	return 1;
}

void do_jump ( int src_fd, int dst_fd, int bitmap_fd,
	       int block_size, long start_block, long end_block,
	       int retry_count, int abort_error, int skip, 
	       int skip_fail, int jump,
	       int good_range, int failed_range,
	       unsigned char * buffer )
{
	long block ;
	long ok_count = 0 ;
	long bad_count = 0 ;

	srandom(getpid() ^ time(NULL));

	// FIXME: figure out how to decide when to quit
	for(;;) {

		block = (long long)random() 
			^ ((long long)random() << 16)
			^ ((long long)random() << 32)
			^ ((long long)random() << 48);
		block %= end_block - start_block;
		if ( block < 0 )
			block += end_block - start_block;
		block += start_block;
		
		if ( ! do_jump_run(src_fd, dst_fd, bitmap_fd,
				   block_size, start_block, end_block,
				   retry_count, abort_error, skip, 
				   skip_fail, jump,
				   good_range, failed_range,
				   block, jump, +1,
				   &ok_count, &bad_count,
				   buffer) )
			if ( abort_error )
				break;

		if ( ! do_jump_run(src_fd, dst_fd, bitmap_fd,
				   block_size, start_block, end_block,
				   retry_count, abort_error, skip, 
				   skip_fail, jump,
				   good_range, failed_range,
				   block-1, jump-1, -1,
				   &ok_count, &bad_count,
				   buffer) )
			if ( abort_error )
				break;

	}
	fprintf(stderr,"\n");
}

const char * usage = 
"myrescue [<options>] <input-file> <output-file>\n"
"options:\n"
"-b <block-size>   block size in bytes, default: 4096\n"
"-B <bitmap-file>  bitmap-file, default: <output-file>.bitmap\n"
"-A                abort on error\n"
"-S                skip errors (exponential-step)\n"
"-f <number>       skip blocks with <number> or more failures\n"
"-r <retry-count>  try up to <retry-count> reads per block, default: 1\n"
"-s <start-block>  start block number, default: 0\n"
"-e <end-block>    end block number (excl.), default: size of <input-file>\n"
"-G <range>        only read <range> blocks around good ones\n"
"-F <range>        skip <range> blocks around failed ones\n"
"-J <number>       randomly jump after reading a few sectors\n"
"-R                reverse copy direction\n"
"-h, -?            usage information\n" ;

int main(int argc, char** argv)
{
	char *src_name ;
	char *dst_name ;
	char *bitmap_name = NULL ;
	char bitmap_suffix[] = ".bitmap" ;
	
	int block_size    = 4096 ;
	int abort_error   = 0 ;
	int skip          = 0 ;
	int skip_fail     = 0 ;
	int retry_count   = 1 ;
	long start_block  = 0 ;
	long end_block    = -1 ;
	int reverse       = 0 ;
	int jump          = 0 ;
	int good_range    = 0 ;
	int failed_range  = 0 ;

	long long block_count ;

	int dst_fd ;
	int src_fd ;
	int bitmap_fd ;

	unsigned char* buffer ;

        int optc ;

	/* options */

        while ( (optc = getopt ( argc, argv, "b:B:ASf:r:s:e:J:G:F:Rh?" ) ) != -1 ) {
		switch ( optc ) {
		case 'b' :
			block_size = atol(optarg);
			if (block_size <= 0) {
				fprintf(stderr, "invalid block-size: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 'B' :
			bitmap_name = optarg;
			break ;
		case 'A' :
			abort_error = 1 ;
			break ;
		case 'S' :
			skip = 1 ;
			break ;
		case 'f' :
			skip_fail = atol(optarg);
			if (skip_fail <= 0) {
				fprintf(stderr, "invalid skip-failed level: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 'r' :
			retry_count = atol(optarg);
			if (retry_count <= 0) {
				fprintf(stderr, "invalid retry-count: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 's' :
			start_block = atol(optarg);
			if (start_block <= 0) {
				fprintf(stderr, "invalid start_block: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 'e' :
			end_block = atol(optarg);
			if (end_block <= 0) {
				fprintf(stderr, "invalid end_block: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 'J' :
			jump = atol(optarg);
			if (jump <= 0) {
				fprintf(stderr, "invalid jump value: %s\n", 
					optarg);
				exit(-1);
			}
			break ;
		case 'G' :
			good_range = atol(optarg);
			if (good_range <= 0) {
				fprintf(stderr, "invalid good range value: %s\n",
					optarg);
				exit(-1);
			}
			break ;
		case 'F' :
			failed_range = atol(optarg);
			if (failed_range <= 0) {
				fprintf(stderr, "invalid failed range value: %s\n",
					optarg);
				exit(-1);
			}
			break ;
		case 'R' :
			reverse = 1 ;
			break ;
                default :
			fprintf ( stderr, "%s", usage ) ;
			exit(-1) ;
		}
	}
        if (optind != argc - 2) {
		fprintf ( stderr, "%s", usage ) ;
		exit(-1) ;
	}

	/* buffer */
	buffer = malloc ( block_size ) ;
	if ( buffer == NULL ) {
		fprintf ( stderr, "malloc (%d) failed\n", block_size ) ;
		exit(-1) ;
	}

	/* filenames */

	src_name = argv[optind] ;
	dst_name = argv[optind+1] ;

	if ( bitmap_name == NULL ) {
		bitmap_name = malloc ( strlen(dst_name) + 
				       strlen(bitmap_suffix) + 1 ) ;
		if ( bitmap_name == NULL ) {
			fprintf ( stderr, "malloc failed\n" ) ;
			exit(-1) ;
		}
		strcpy ( bitmap_name, dst_name ) ;
		strcat ( bitmap_name, bitmap_suffix ) ;
	}

	/* open files */

	src_fd = open64(src_name, O_RDONLY);
	if ( src_fd < 0 ) {
		perror ( "source open failed" ) ;
		exit(-1) ;
	}

	dst_fd = open64(dst_name, O_RDWR | O_CREAT, 0600);
	if ( dst_fd < 0 ) {
		perror ( "destination open failed" ) ;
		exit(-1) ;
	}

	bitmap_fd = open64(bitmap_name, O_RDWR | O_CREAT, 0600);
	if ( bitmap_fd < 0 ) {
		perror ( "bitmap open failed" ) ;
		exit(-1) ;
	}

	/* maximum block */
	block_count = filesize(src_fd) ;
	block_count /= block_size ;

	if ( end_block == -1 ) {
		end_block = block_count ;
	} 
#ifdef CHECK_END_BLOCK
	if ( end_block > block_count ) {
		fprintf ( stderr, 
			  "end_block(%ld) > block_count(%Ld)\n"
			  "end_block clipped\n",
			  end_block, block_count ) ;
		end_block = block_count ;
	}
#endif
	if ( start_block >= end_block ) {
		fprintf ( stderr, 
			  "start_block(%ld) >= end_block(%ld)\n",
			  start_block, end_block ) ;
		exit(-1) ;
	}

	/* start the real job */
	if ( jump == 0 )
		do_copy ( src_fd, dst_fd, bitmap_fd,
			  block_size, start_block, end_block,
			  retry_count, abort_error, skip, 
			  skip_fail, reverse,
			  good_range, failed_range,
			  buffer ) ;
	else
		do_jump ( src_fd, dst_fd, bitmap_fd,
			  block_size, start_block, end_block,
			  retry_count, abort_error, skip, 
			  skip_fail, jump,
			  good_range, failed_range,
			  buffer );
	return 0 ;
}
